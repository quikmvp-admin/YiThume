# api/app.py
import os
import uuid
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson.objectid import ObjectId

# -----------------------
# CONFIG
# -----------------------

# Get secrets from env vars (never hardcode in code you push to GitHub)
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)

client = MongoClient(MONGO_URI)
db = client.yithume  # database name "yithume"

app = Flask(__name__)
CORS(app)  # allow browser to call these endpoints from your frontend


# -----------------------
# HELPERS
# -----------------------

def make_order_id():
    """Generate a human-readable order ref like YI-20251024-ABC123"""
    return f"YI-{datetime.utcnow().strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"


def inside_service_area(lat, lng):
    """
    VERY SIMPLE service area check:
    You can tighten this later. For now we just roughly bound Eastern Cape towns
    you said you're serving (Port Alfred / Bathurst / Kenton-on-Sea).
    If coords are missing, just say True so we don't block legit orders.
    """
    if lat is None or lng is None:
        return True

    # Example loose bounding box around Port Alfred / Kenton / Bathurst area
    # You should tweak this.
    min_lat, max_lat = -34.2, -33.0
    min_lng, max_lng = 25.5, 27.5

    return (min_lat <= lat <= max_lat) and (min_lng <= lng <= max_lng)


def rule_based_score(order_doc):
    """
    Cheap fraud heuristics.
    Returns (score, flags)
    score is 0 -> 1
    flags explain WHY it was flagged so you can review it in dashboard.
    """
    score = 0.0
    flags = {}

    phone = order_doc.get("customer", {}).get("phone")
    total_value = order_doc.get("total", 0)

    # 1. Phone velocity: same phone spamming orders in last hour
    if phone:
        recent_same_phone = db.orders.count_documents({
            "customer.phone": phone,
            "created_at": {"$gte": datetime.utcnow() - timedelta(hours=1)}
        })
        if recent_same_phone >= 3:
            flags["phone_velocity"] = True
            score += 0.4

        # 2. Duplicate order within last 10 min (same phone + same subtotal)
        recent_dup = db.orders.find_one({
            "customer.phone": phone,
            "subtotal": order_doc.get("subtotal", 0),
            "created_at": {"$gte": datetime.utcnow() - timedelta(minutes=10)}
        })
        if recent_dup:
            flags["duplicate_order"] = True
            score += 0.3

    # 3. Address outside service area
    coords = (
        order_doc.get("customer", {})
                 .get("address", {})
                 .get("coords", {})
    )

    lat = coords.get("lat")
    lng = coords.get("lng")
    if lat is not None and lng is not None and not inside_service_area(lat, lng):
        flags["address_out_of_area"] = True
        score += 0.5

    # 4. High order value relative to simple average
    # We'll compute avg total fast. If no orders yet, default avg_total = 50.
    pipeline = [{"$group": {"_id": None, "avg": {"$avg": "$total"}}}]
    agg = list(db.orders.aggregate(pipeline))
    avg_total = agg[0]["avg"] if agg else 50

    if total_value > avg_total * 3:
        flags["high_value"] = True
        score += 0.2

    # cap at 1.0
    score = min(score, 1.0)

    return score, flags


def serialize_order(doc):
    """Convert Mongo ObjectId etc so we can JSON it easily."""
    if not doc:
        return None
    doc["_id"] = str(doc["_id"])
    if doc.get("assigned_driver_id"):
        doc["assigned_driver_id"] = str(doc["assigned_driver_id"])
    return doc


def log_audit(entity, entity_id, action, payload, by="system"):
    db.audit_logs.insert_one({
        "entity": entity,
        "entity_id": entity_id,
        "action": action,
        "payload": payload,
        "by": by,
        "ts": datetime.utcnow()
    })


# -----------------------
# ROUTES
# -----------------------

@app.route("/", methods=["GET"])
def healthcheck():
    return jsonify({"ok": True, "service": "YiThume backend"}), 200


@app.route("/orders", methods=["POST"])
def create_order():
    """
    Create a new order.
    Expected JSON body shape:
    {
      "customer": { "name": "...", "phone": "...", "address": { "line1": "...", "suburb": "...", "coords": { "lat": -33.5, "lng": 26.9 } } },
      "items": [ { "sku": "...", "name": "...", "qty": 2, "price": 25 } ],
      "subtotal": 50,
      "delivery_fee": 20,
      "total": 70,
      "payment": { "method":"cash","status":"pending" },
      "created_by": "whatsapp" | "web" | "admin",
      "meta": { any extra stuff }
    }
    """
    data = request.json or {}

    order_doc = {
        "order_id": make_order_id(),
        "created_at": datetime.utcnow(),
        "customer": data.get("customer", {}),
        "items": data.get("items", []),
        "subtotal": data.get("subtotal", 0),
        "delivery_fee": data.get("delivery_fee", 0),
        "total": data.get("total", 0),
        "payment": data.get("payment", {
            "method": "cash",
            "status": "pending",
            "provider_ref": None
        }),
        "status": "pending",
        "assigned_driver_id": None,
        "assigned_at": None,
        "delivered_at": None,
        "route": data.get("route", {}),
        "fraud_score": 0.0,
        "fraud_flags": {},
        "created_by": data.get("created_by", "web"),
        "meta": data.get("meta", {})
    }

    # insert draft
    res = db.orders.insert_one(order_doc)
    oid = res.inserted_id

    # run fraud rules, update
    score, flags = rule_based_score(order_doc)
    order_status = "pending"
    if score >= 0.75:
        order_status = "review_required"

    db.orders.update_one(
        {"_id": oid},
        {"$set": {
            "fraud_score": score,
            "fraud_flags": flags,
            "status": order_status
        }}
    )

    # audit log
    log_audit("orders", oid, "create", {**order_doc, "fraud_score": score, "fraud_flags": flags})

    return jsonify({
        "ok": True,
        "order_db_id": str(oid),
        "order_public_id": order_doc["order_id"],
        "status": order_status,
        "fraud_score": score,
        "fraud_flags": flags
    }), 201


@app.route("/orders", methods=["GET"])
def list_orders():
    """
    For admin dashboard: show recent orders.
    Optional query params: status=pending|assigned|...
    """
    q = {}
    status_filter = request.args.get("status")
    if status_filter:
        q["status"] = status_filter

    cursor = db.orders.find(q).sort("created_at", -1).limit(50)
    orders = [serialize_order(o) for o in cursor]
    return jsonify({"ok": True, "orders": orders}), 200


@app.route("/orders/<order_id>/assign", methods=["POST"])
def assign_driver(order_id):
    """
    Assign a driver to an order.
    Body: { "driver_id": "<mongo _id of driver>" }
    """
    body = request.json or {}
    driver_id_str = body.get("driver_id")
    if not driver_id_str:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    try:
        oid = ObjectId(order_id)
        did = ObjectId(driver_id_str)
    except Exception:
        return jsonify({"ok": False, "error": "bad ObjectId"}), 400

    db.orders.update_one(
        {"_id": oid},
        {"$set": {
            "assigned_driver_id": did,
            "assigned_at": datetime.utcnow(),
            "status": "assigned"
        }}
    )

    log_audit("orders", oid, "assign_driver", {"driver_id": driver_id_str})

    return jsonify({"ok": True}), 200


@app.route("/orders/<order_id>/status", methods=["POST"])
def update_status(order_id):
    """
    Update delivery status.
    Body: { "status": "in_transit" | "delivered" | "cancelled" | "failed" }
    """
    body = request.json or {}
    new_status = body.get("status")
    if new_status not in ["pending", "assigned", "in_transit", "delivered", "cancelled", "failed", "review_required"]:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    try:
        oid = ObjectId(order_id)
    except Exception:
        return jsonify({"ok": False, "error": "bad ObjectId"}), 400

    update_set = {"status": new_status}
    if new_status == "delivered":
        update_set["delivered_at"] = datetime.utcnow()

    db.orders.update_one(
        {"_id": oid},
        {"$set": update_set}
    )

    log_audit("orders", oid, "status_change", {"status": new_status})

    return jsonify({"ok": True}), 200


@app.route("/drivers", methods=["POST"])
def create_driver():
    """
    Add a driver.
    Body:
    {
      "driver_id": "DRV-001",
      "name": "Thabo",
      "phone": "+27...",
      "vehicle": "motorbike",
      "available": true,
      "current_location": { "lat": -33.5, "lng": 26.9 }
    }
    """
    data = request.json or {}
    driver_doc = {
        "driver_id": data.get("driver_id"),
        "name": data.get("name"),
        "phone": data.get("phone"),
        "vehicle": data.get("vehicle", "car"),
        "active": True,
        "available": data.get("available", True),
        "current_location": {
            "lat": data.get("current_location", {}).get("lat"),
            "lng": data.get("current_location", {}).get("lng"),
            "updated_at": datetime.utcnow()
        },
        "weekly_payout_due": 0.0,
        "earnings_history": [],
        "ratings": { "count": 0, "avg": None }
    }

    res = db.drivers.insert_one(driver_doc)
    log_audit("drivers", res.inserted_id, "create_driver", driver_doc)
    return jsonify({"ok": True, "driver_db_id": str(res.inserted_id)}), 201


@app.route("/drivers", methods=["GET"])
def list_drivers():
    drivers = db.drivers.find({"active": True})
    out = []
    for d in drivers:
        d["_id"] = str(d["_id"])
        out.append(d)
    return jsonify({"ok": True, "drivers": out}), 200


# -----------------------
# LOCAL DEV ENTRYPOINT
# -----------------------
# For local dev: `python api/app.py` (or `flask run` etc)
if __name__ == "__main__":
    # When running locally you can hit http://127.0.0.1:5000/orders etc.
    app.run(host="0.0.0.0", port=5000, debug=True)
