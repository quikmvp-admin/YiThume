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

MONGO_URI = os.environ.get(
    "MONGO_URI",
    # fallback so it doesn't instantly crash if env var isn't set in Vercel yet
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)

client = MongoClient(MONGO_URI)
db = client.yithume  # DB name

app = Flask(__name__)
CORS(app)


# -----------------------
# HELPERS
# -----------------------

def make_order_id():
    return f"YI-{datetime.utcnow().strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"

def inside_service_area(lat, lng):
    # loose check, doesn't block legit orders if coords missing
    if lat is None or lng is None:
        return True

    # rough EC bounding box
    min_lat, max_lat = -34.2, -33.0
    min_lng, max_lng = 25.5, 27.5
    return (min_lat <= lat <= max_lat) and (min_lng <= lng <= max_lng)

def rule_based_score(order_doc):
    score = 0.0
    flags = {}

    phone = order_doc.get("customer", {}).get("phone")
    total_value = order_doc.get("total", 0)

    # 1. phone velocity
    if phone:
        recent_same_phone = db.orders.count_documents({
            "customer.phone": phone,
            "created_at": {"$gte": datetime.utcnow() - timedelta(hours=1)}
        })
        if recent_same_phone >= 3:
            flags["phone_velocity"] = True
            score += 0.4

        # 2. duplicate in last 10 min
        recent_dup = db.orders.find_one({
            "customer.phone": phone,
            "subtotal": order_doc.get("subtotal", 0),
            "created_at": {"$gte": datetime.utcnow() - timedelta(minutes=10)}
        })
        if recent_dup:
            flags["duplicate_order"] = True
            score += 0.3

    # 3. service area
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

    # 4. high order value
    pipeline = [{"$group": {"_id": None, "avg": {"$avg": "$total"}}}]
    agg = list(db.orders.aggregate(pipeline))
    avg_total = agg[0]["avg"] if agg else 50

    if total_value > avg_total * 3:
        flags["high_value"] = True
        score += 0.2

    return min(score, 1.0), flags


def serialize_order(doc):
    if not doc:
        return None
    doc["_id"] = str(doc["_id"])
    if doc.get("assigned_driver_id"):
        doc["assigned_driver_id"] = str(doc["assigned_driver_id"])
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
#
# We mount each route twice:
#   1. "/api/app/..."    <- what the browser will call (BASE_API=/api/app)
#   2. "/.../..."        <- fallback/testing
#
# Vercel will route /api/app/... to this file because of vercel.json.
#

@app.route("/", methods=["GET"])
@app.route("/api/app", methods=["GET"])
def healthcheck():
    return jsonify({"ok": True, "service": "YiThume backend"}), 200


# --- CREATE ORDER ---
@app.route("/orders", methods=["POST"])
@app.route("/api/app/orders", methods=["POST"])
def create_order():
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

    # fraud rules
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


# --- LIST ORDERS ---
@app.route("/orders", methods=["GET"])
@app.route("/api/app/orders", methods=["GET"])
def list_orders():
    status_filter = request.args.get("status")

    q = {}
    if status_filter:
        q["status"] = status_filter

    cursor = db.orders.find(q).sort("created_at", -1).limit(50)
    orders = [serialize_order(o) for o in cursor]

    return jsonify({"ok": True, "orders": orders}), 200


# --- ASSIGN DRIVER ---
@app.route("/orders/<oid>/assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/assign", methods=["POST"])
def assign_driver(oid):
    body = request.json or {}
    driver_id_str = body.get("driver_id")
    if not driver_id_str:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    try:
        order_obj_id = ObjectId(oid)
        driver_obj_id = ObjectId(driver_id_str)
    except Exception:
        return jsonify({"ok": False, "error": "bad ObjectId"}), 400

    db.orders.update_one(
        {"_id": order_obj_id},
        {"$set": {
            "assigned_driver_id": driver_obj_id,
            "assigned_at": datetime.utcnow(),
            "status": "assigned"
        }}
    )

    log_audit("orders", order_obj_id, "assign_driver",
              {"driver_id": driver_id_str})

    return jsonify({"ok": True}), 200


# --- UPDATE STATUS ---
@app.route("/orders/<oid>/status", methods=["POST"])
@app.route("/api/app/orders/<oid>/status", methods=["POST"])
def update_status(oid):
    body = request.json or {}
    new_status = body.get("status")

    allowed_status = [
        "pending", "assigned", "in_transit",
        "delivered", "cancelled", "failed",
        "review_required"
    ]
    if new_status not in allowed_status:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    try:
        order_obj_id = ObjectId(oid)
    except Exception:
        return jsonify({"ok": False, "error": "bad ObjectId"}), 400

    update_set = {"status": new_status}
    if new_status == "delivered":
        update_set["delivered_at"] = datetime.utcnow()

    db.orders.update_one(
        {"_id": order_obj_id},
        {"$set": update_set}
    )

    log_audit("orders", order_obj_id, "status_change",
              {"status": new_status})

    return jsonify({"ok": True}), 200


# --- CREATE DRIVER ---
@app.route("/drivers", methods=["POST"])
@app.route("/api/app/drivers", methods=["POST"])
def create_driver():
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

    return jsonify({
        "ok": True,
        "driver_db_id": str(res.inserted_id)
    }), 201


# --- LIST DRIVERS ---
@app.route("/drivers", methods=["GET"])
@app.route("/api/app/drivers", methods=["GET"])
def list_drivers():
    driver_cursor = db.drivers.find({"active": True})
    out = []
    for d in driver_cursor:
        d["_id"] = str(d["_id"])
        out.append(d)
    return jsonify({"ok": True, "drivers": out}), 200


# IMPORTANT:
# DO NOT put "if __name__ == '__main__': app.run(...)" here.
# On Vercel, this file is imported, not run like a normal server process.
