# api/app.py
import os
import uuid
import random
import time
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson.objectid import ObjectId

# -----------------------
# CONFIG
# -----------------------

# IMPORTANT:
# make sure you set MONGO_URI in Vercel project settings → Environment Variables
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)

client = MongoClient(MONGO_URI)
db = client.yithume  # MongoDB database "yithume"

app = Flask(__name__)
CORS(app)  # allow browser to hit these routes

# All routes are served under this base path to match frontend fetch("/api/app/..."):
BASE_PATH = "/api/app"


# -----------------------
# HELPERS
# -----------------------

def make_order_id():
    """
    Generate a public-facing order ref.
    Example: YI-20251024-3F9A2C
    """
    return f"YI-{datetime.utcnow().strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"


def inside_service_area(lat, lng):
    """
    Very basic geofence to catch obvious scams.
    You can tune this later.
    For now, rough Eastern Cape block.
    If coords are missing, don't block.
    """
    if lat is None or lng is None:
        return True

    # tweak bounds as needed — this is just Port Alfred / Kenton / Bathurst-ish
    min_lat, max_lat = -34.2, -33.0
    min_lng, max_lng = 25.5, 27.5

    return (min_lat <= lat <= max_lat) and (min_lng <= lng <= max_lng)


def rule_based_score(order_doc):
    """
    Simple fraud scoring.
    Returns (score, flags dict).
    """
    score = 0.0
    flags = {}

    phone = order_doc.get("customer", {}).get("phone")
    total_value = order_doc.get("total", 0)

    # 1. Phone velocity = spam orders from same number in last hour
    if phone:
        recent_same_phone = db.orders.count_documents({
            "customer.phone": phone,
            "created_at": {"$gte": datetime.utcnow() - timedelta(hours=1)}
        })
        if recent_same_phone >= 3:
            flags["phone_velocity"] = True
            score += 0.4

        # 2. Duplicate order (same subtotal) in last 10 mins
        recent_dup = db.orders.find_one({
            "customer.phone": phone,
            "subtotal": order_doc.get("subtotal", 0),
            "created_at": {"$gte": datetime.utcnow() - timedelta(minutes=10)}
        })
        if recent_dup:
            flags["duplicate_order"] = True
            score += 0.3

    # 3. Address out of area
    coords = (
        order_doc
        .get("customer", {})
        .get("address", {})
        .get("coords", {})
    )
    lat = coords.get("lat")
    lng = coords.get("lng")
    if lat is not None and lng is not None and not inside_service_area(lat, lng):
        flags["address_out_of_area"] = True
        score += 0.5

    # 4. High total vs avg total
    pipeline = [{"$group": {"_id": None, "avg": {"$avg": "$total"}}}]
    agg = list(db.orders.aggregate(pipeline))
    avg_total = agg[0]["avg"] if agg else 50  # fallback avg 50

    if total_value > avg_total * 3:
        flags["high_value"] = True
        score += 0.2

    # Cap the score at 1.0
    score = min(score, 1.0)
    return score, flags


def serialize_order(doc):
    """
    Convert Mongo ObjectId etc into JSON-safe types.
    Only return what the frontend dashboard actually needs.
    """
    if not doc:
        return None

    out = {
        "order_public_id": doc.get("order_id"),
        "order_id": str(doc.get("_id")),
        "status": doc.get("status"),
        "total": doc.get("total"),
        "items": doc.get("items", []),
        "meta": doc.get("meta", {}),
        "fraud_score": doc.get("fraud_score", 0.0),
        "created_at": doc.get("created_at").isoformat() if doc.get("created_at") else None,
        "customer": {
            "name": doc.get("customer", {}).get("name"),
            "phone": doc.get("customer", {}).get("phone"),
            "address": doc.get("customer", {}).get("address"),
        },
        "payment": doc.get("payment", {}),
    }

    driver_id = doc.get("assigned_driver_id")
    if driver_id:
        out["assigned_driver_id"] = str(driver_id)

    return out


def log_audit(entity, entity_id, action, payload, by="system"):
    try:
        db.audit_logs.insert_one({
            "entity": entity,
            "entity_id": str(entity_id),
            "action": action,
            "payload": payload,
            "by": by,
            "ts": datetime.utcnow()
        })
    except Exception as e:
        # don't kill request just because audit log failed
        print("audit log failed:", e)


# -----------------------
# ROUTES
# -----------------------

@app.route(f"{BASE_PATH}", methods=["GET"])
def healthcheck():
    """
    Sanity check route.
    Frontend can call GET /api/app to test if backend is alive.
    """
    return jsonify({
        "ok": True,
        "service": "YiThume backend",
        "ts": datetime.utcnow().isoformat()
    }), 200


@app.route(f"{BASE_PATH}/orders", methods=["POST"])
def create_order():
    """
    Create a new order.

    Expected JSON (from frontend):
    {
      "customer": {
        "name": "...",
        "phone": "...",
        "address": {
          "line1": "...",
          "suburb": "...",
          "coords": { "lat": -33.5, "lng": 26.9 }
        }
      },
      "items": [
        { "sku": null, "name": "Panado", "qty": 2, "price": 35 }
      ],
      "subtotal": 70,
      "delivery_fee": 25,
      "total": 95,
      "payment": { "method":"card","status":"pending","provider_ref": null },
      "created_by": "web",
      "route": { "eta_minutes": null, "distance_km": null, "eta_text": "..." },
      "meta": { "rush": true, "zone": "A" }
    }
    """
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    # Basic validation
    if "customer" not in data or "items" not in data:
        return jsonify({"ok": False, "error": "missing_fields"}), 400

    # Build the order document that will be stored in MongoDB
    order_doc = {
        "order_id": make_order_id(),      # public ref like YI-20251024-ABC123
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
        "status": "pending",              # may change to review_required below
        "assigned_driver_id": None,
        "assigned_at": None,
        "delivered_at": None,
        "route": data.get("route", {}),
        "fraud_score": 0.0,
        "fraud_flags": {},
        "created_by": data.get("created_by", "web"),
        "meta": data.get("meta", {})
    }

    # Insert a draft just so we can include it in fraud comparisons
    insert_res = db.orders.insert_one(order_doc)
    oid = insert_res.inserted_id

    # Fraud scoring
    score, flags = rule_based_score(order_doc)
    status = "pending"
    if score >= 0.75:
        status = "review_required"

    # Update the stored doc with the fraud info + possibly new status
    db.orders.update_one(
        {"_id": oid},
        {"$set": {
            "fraud_score": score,
            "fraud_flags": flags,
            "status": status
        }}
    )

    # Audit trail
    log_audit(
        entity="orders",
        entity_id=oid,
        action="create",
        payload={**order_doc, "fraud_score": score, "fraud_flags": flags}
    )

    # Respond in the shape the frontend expects
    return jsonify({
        "ok": True,
        "order_db_id": str(oid),
        "order_public_id": order_doc["order_id"],
        "status": status,
        "fraud_score": score,
        "fraud_flags": flags
    }), 201


@app.route(f"{BASE_PATH}/orders", methods=["GET"])
def list_orders():
    """
    Admin dashboard /orders list.

    Optional query param:
      ?status=pending
    """
    status_filter = request.args.get("status")
    q = {}
    if status_filter:
        q["status"] = status_filter

    cursor = (
        db.orders
        .find(q)
        .sort("created_at", -1)
        .limit(50)
    )

    orders_out = [serialize_order(o) for o in cursor]

    return jsonify({
        "ok": True,
        "orders": orders_out
    }), 200


@app.route(f"{BASE_PATH}/orders/<order_oid>/assign", methods=["POST"])
def assign_driver(order_oid):
    """
    Assign a driver to an order.
    Body: { "driver_id": "<mongo _id of driver>" }
    """
    try:
        body = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    driver_id_str = body.get("driver_id")
    if not driver_id_str:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    try:
        oid = ObjectId(order_oid)
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

    log_audit(
        "orders",
        oid,
        "assign_driver",
        {"driver_id": driver_id_str}
    )

    return jsonify({"ok": True}), 200


@app.route(f"{BASE_PATH}/orders/<order_oid>/status", methods=["POST"])
def update_status(order_oid):
    """
    Update delivery status.
    Body: { "status": "in_transit" | "delivered" | "cancelled" | "failed" | ... }
    """
    try:
        body = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    new_status = body.get("status")
    if new_status not in [
        "pending", "review_required",
        "assigned", "in_transit",
        "delivered", "cancelled", "failed"
    ]:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    try:
        oid = ObjectId(order_oid)
    except Exception:
        return jsonify({"ok": False, "error": "bad ObjectId"}), 400

    update_set = {"status": new_status}
    if new_status == "delivered":
        update_set["delivered_at"] = datetime.utcnow()

    db.orders.update_one({"_id": oid}, {"$set": update_set})

    log_audit(
        "orders",
        oid,
        "status_change",
        {"status": new_status}
    )

    return jsonify({"ok": True}), 200


@app.route(f"{BASE_PATH}/drivers", methods=["POST"])
def create_driver():
    """
    Add a driver from the signup modal.

    Expected body:
    {
      "driver_id": "DRV-123456",
      "name": "Thabo",
      "phone": "2782...",
      "vehicle": "motorbike",
      "available": true,
      "current_location": { "lat": -33.5, "lng": 26.9 },
      "meta": { "zone":"A", "radius_km":"10" }
    }
    """
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    # minimum sanity check
    if not data.get("name") or not data.get("phone"):
        return jsonify({"ok": False, "error": "missing name/phone"}), 400

    driver_doc = {
        "driver_id": data.get("driver_id") or f"DRV-{random.randint(100000,999999)}",
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
        "ratings": {"count": 0, "avg": None},
        "meta": data.get("meta", {}),
        "created_ts": time.time()
    }

    res = db.drivers.insert_one(driver_doc)

    log_audit(
        "drivers",
        res.inserted_id,
        "create_driver",
        driver_doc
    )

    return jsonify({
        "ok": True,
        "driver_db_id": str(res.inserted_id),
        "driver_id": driver_doc["driver_id"]
    }), 201


@app.route(f"{BASE_PATH}/drivers", methods=["GET"])
def list_drivers():
    """
    Return all active drivers.
    """
    cursor = db.drivers.find({"active": True}).sort("created_ts", -1)
    out = []
    for d in cursor:
        d["_id"] = str(d["_id"])
        out.append(d)
    return jsonify({"ok": True, "drivers": out}), 200


# -----------------------
# LOCAL DEV ENTRYPOINT
# -----------------------
# For local dev you can:
#   python api/app.py
# and hit http://127.0.0.1:5000/api/app/orders
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
