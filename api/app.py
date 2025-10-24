import os
import uuid
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from pymongo import MongoClient
from bson.objectid import ObjectId

# -----------------------
# CONFIG
# -----------------------

# IMPORTANT: set this in Vercel → Project Settings → Environment Variables
#   MONGO_URI = your full mongodb+srv://... string
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)

client = MongoClient(MONGO_URI)
db = client.yithume  # database name "yithume"

app = Flask(__name__, template_folder="templates")
CORS(app)  # still fine; does nothing bad even on same-origin


# -----------------------
# HELPERS
# -----------------------

def make_order_id():
    """Generate a public-friendly order ref like YI-20251024-ABC123"""
    return f"YI-{datetime.utcnow().strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"


def inside_service_area(lat, lng):
    """
    Quick service area check around Eastern Cape focus.
    If coords missing, allow.
    """
    if lat is None or lng is None:
        return True

    # loose bounding box around Port Alfred / Kenton / Bathurst etc.
    # tweak for realism later
    min_lat, max_lat = -34.2, -33.0
    min_lng, max_lng = 25.5, 27.5

    return (min_lat <= lat <= max_lat) and (min_lng <= lng <= max_lng)


def rule_based_score(order_doc):
    """
    Dumb fraud heuristics.
    Returns (score, flags)
    score is between 0 and 1.
    """
    score = 0.0
    flags = {}

    phone = order_doc.get("customer", {}).get("phone")
    total_value = order_doc.get("total", 0)

    # 1. Phone velocity: same number spamming orders in last hour
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

    # 4. High order value
    pipeline = [{"$group": {"_id": None, "avg": {"$avg": "$total"}}}]
    agg = list(db.orders.aggregate(pipeline))
    avg_total = agg[0]["avg"] if agg else 50

    if total_value > avg_total * 3:
        flags["high_value"] = True
        score += 0.2

    return min(score, 1.0), flags


def iso(dt):
    if isinstance(dt, datetime):
        # always return a string so jsonify won't choke
        return dt.isoformat() + "Z"
    return dt


def serialize_order(doc):
    """Convert Mongo ObjectIds + datetimes so we can send to frontend."""
    if not doc:
        return None

    doc["_id"] = str(doc["_id"])

    # string-ify ObjectIds
    if doc.get("assigned_driver_id"):
        doc["assigned_driver_id"] = str(doc["assigned_driver_id"])

    # make datetimes JSON safe
    for field in ["created_at", "assigned_at", "delivered_at"]:
        if field in doc:
            doc[field] = iso(doc[field])

    # also fix nested date in driver location etc if it exists
    if "route" in doc and isinstance(doc["route"], dict):
        # nothing time-based in route yet, keep as-is
        pass

    return doc


def log_audit(entity, entity_id, action, payload, by="system"):
    db.audit_logs.insert_one({
        "entity": entity,
        "entity_id": str(entity_id),
        "action": action,
        "payload": payload,
        "by": by,
        "ts": datetime.utcnow()
    })


# -----------------------
# ROUTES
# -----------------------

@app.route("/", methods=["GET"])
def home_page():
    # Serve the landing page+modal etc.
    # NOTE: index.html must live in /templates/index.html
    return render_template("index.html")


@app.route("/orders", methods=["POST"])
def create_order():
    """
    Create a new order. Body shape (from the site JS):
    {
      "customer": {
        "name": "...",
        "phone": "...",
        "address": {
          "line1": "...",
          "suburb": "Zone A",
          "coords": { "lat": -33.59, "lng": 26.89 }
        }
      },
      "items": [ { "name":"Panado", "qty":2, "price":35 }, ... ],
      "subtotal": 50,
      "delivery_fee": 20,
      "total": 70,
      "payment": { "method":"card","status":"pending","provider_ref":null },
      "created_by": "web",
      "route": { "eta_minutes":null, "distance_km":null, "eta_text":"Wed 12:00–14:00" },
      "meta": { "rush": true, "zone": "A" }
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

    # save draft first
    res = db.orders.insert_one(order_doc)
    oid = res.inserted_id

    # run fraud check + maybe bump status
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

    # audit
    log_audit("orders", oid, "create", {
        **order_doc,
        "fraud_score": score,
        "fraud_flags": flags
    })

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
    Admin dashboard pulls by status for tabs:
    /orders?status=pending
    /orders?status=review_required
    /orders?status=assigned
    /orders?status=delivered
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
    Assign a driver (future feature).
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
    Set order status.
    Body: { "status": "in_transit" | "delivered" | "cancelled" | "failed" | "review_required" | "pending" | "assigned" }
    """
    body = request.json or {}
    new_status = body.get("status")
    allowed = [
        "pending", "assigned", "in_transit",
        "delivered", "cancelled", "failed",
        "review_required"
    ]
    if new_status not in allowed:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    try:
        oid = ObjectId(order_id)
    except Exception:
        return jsonify({"ok": False, "error": "bad ObjectId"}), 400

    update_set = {"status": new_status}
    if new_status == "delivered":
        update_set["delivered_at"] = datetime.utcnow()

    db.orders.update_one({"_id": oid}, {"$set": update_set})

    log_audit("orders", oid, "status_change", {"status": new_status})
    return jsonify({"ok": True}), 200


@app.route("/drivers", methods=["POST"])
def create_driver():
    """
    Add a driver from the signup modal.
    Body:
    {
      "driver_id": "DRV-123456",
      "name": "Thabo",
      "phone": "2782...",
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
        "ratings": {"count": 0, "avg": None}
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
        loc = d.get("current_location", {})
        if "updated_at" in loc:
            loc["updated_at"] = iso(loc["updated_at"])
        out.append(d)

    return jsonify({"ok": True, "drivers": out}), 200


# -----------------------
# LOCAL DEV ENTRYPOINT
# -----------------------
if __name__ == "__main__":
    # local testing:
    #   export MONGO_URI="mongodb+srv://..."
    #   pip install -r requirements.txt
    #   python app.py
    app.run(host="0.0.0.0", port=5000, debug=True)
