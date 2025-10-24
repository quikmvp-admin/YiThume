# app.py
import os
import uuid
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson.objectid import ObjectId

#
# --- CONFIG ---
#

# IMPORTANT:
# In Vercel dashboard > Settings > Environment Variables
#   Name: MONGO_URI
#   Value: your real Mongo connection string
#
MONGO_URI = os.environ.get(
    "MONGO_URI",
    # fallback only for local dev; DO NOT leave real creds here when you push
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)

client = MongoClient(MONGO_URI)
db = client.yithume  # DB name

app = Flask(__name__)

# allow browser JS from same domain (and also local dev)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)


#
# --- HELPERS ---
#

def make_order_id():
    """Generate something human-readable, e.g. YI-20251024-ABC123"""
    return f"YI-{datetime.utcnow().strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"


def inside_service_area(lat, lng):
    """
    Very loose geo fence. You can tighten this later.
    If coords missing -> allow.
    """
    if lat is None or lng is None:
        return True

    # Rough Eastern Cape-ish bounding box; adjust for real ops
    min_lat, max_lat = -34.2, -33.0
    min_lng, max_lng = 25.5, 27.5

    return (min_lat <= lat <= max_lat) and (min_lng <= lng <= max_lng)


def rule_based_score(order_doc):
    """
    Quick fraud heuristics.
    Returns (score, flags)
    score: 0.0 -> 1.0
    flags: dict of reasons
    """
    score = 0.0
    flags = {}

    phone = order_doc.get("customer", {}).get("phone")
    total_value = order_doc.get("total", 0)

    # 1. Phone velocity: same phone spamming in last hour
    if phone:
        recent_same_phone = db.orders.count_documents({
            "customer.phone": phone,
            "created_at": {"$gte": datetime.utcnow() - timedelta(hours=1)}
        })
        if recent_same_phone >= 3:
            flags["phone_velocity"] = True
            score += 0.4

        # 2. Duplicate-ish order in last 10 minutes (same phone+subtotal)
        recent_dup = db.orders.find_one({
            "customer.phone": phone,
            "subtotal": order_doc.get("subtotal", 0),
            "created_at": {"$gte": datetime.utcnow() - timedelta(minutes=10)}
        })
        if recent_dup:
            flags["duplicate_order"] = True
            score += 0.3

    # 3. Out of area
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

    # 4. High value relative to average basket
    pipeline = [{"$group": {"_id": None, "avg": {"$avg": "$total"}}}]
    agg = list(db.orders.aggregate(pipeline))
    avg_total = agg[0]["avg"] if agg else 50  # default avg R50 if no history

    if total_value > avg_total * 3:
        flags["high_value"] = True
        score += 0.2

    # clamp
    score = min(score, 1.0)

    return score, flags


def serialize_order(doc):
    """Mongo -> JSON-safe dict."""
    if not doc:
        return None
    doc["_id"] = str(doc["_id"])
    if doc.get("assigned_driver_id"):
        doc["assigned_driver_id"] = str(doc["assigned_driver_id"])
    # ObjectIds etc are gone now, safe to send
    return doc


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
        # don't kill the request just because audit failed
        print("audit log failed:", e)


#
# --- ROUTES ---
#

@app.route("/", methods=["GET"])
def healthcheck():
    # simple sanity ping
    return jsonify({"ok": True, "service": "YiThume backend"}), 200


@app.route("/orders", methods=["POST"])
def create_order():
    """
    Create a new order from website checkout.
    Body example:
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
      "items": [
        {"name": "Panado", "qty": 2, "price": 35}
      ],
      "subtotal": 70,
      "delivery_fee": 25,
      "total": 95,
      "payment": {
        "method": "card" | "eft" | "deposit_cod",
        "status": "pending",
        "provider_ref": null
      },
      "created_by": "web",
      "route": {
        "eta_minutes": null,
        "distance_km": null,
        "eta_text": "12:00–14:00 today"
      },
      "meta": {
        "rush": true,
        "zone": "A"
      }
    }
    """
    data = request.json or {}
    print("Incoming /orders payload:", data)

    # build order doc for DB
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

    # insert draft to Mongo
    try:
        res = db.orders.insert_one(order_doc)
    except Exception as e:
        print("Mongo insert failed:", e)
        return jsonify({
            "ok": False,
            "error": "db_insert_failed",
            "details": str(e)
        }), 500

    oid = res.inserted_id

    # run fraud & update
    score, flags = rule_based_score(order_doc)
    new_status = "pending"
    if score >= 0.75:
        new_status = "review_required"

    try:
        db.orders.update_one(
            {"_id": oid},
            {"$set": {
                "fraud_score": score,
                "fraud_flags": flags,
                "status": new_status
            }}
        )
    except Exception as e:
        print("fraud update failed:", e)

    # audit
    log_audit("orders", oid, "create", {
        **order_doc,
        "fraud_score": score,
        "fraud_flags": flags
    })

    # final response for frontend
    return jsonify({
        "ok": True,
        "order_db_id": str(oid),
        "order_public_id": order_doc["order_id"],
        "status": new_status,
        "fraud_score": score,
        "fraud_flags": flags
    }), 201


@app.route("/orders", methods=["GET"])
def list_orders():
    """
    Admin dashboard fetch.
    Optional: ?status=pending|review_required|assigned|delivered...
    """
    q = {}
    status_filter = request.args.get("status")
    if status_filter:
        q["status"] = status_filter

    cursor = db.orders.find(q).sort("created_at", -1).limit(50)
    orders = [serialize_order(o) for o in cursor]

    return jsonify({"ok": True, "orders": orders}), 200


@app.route("/orders/<mongo_id>/assign", methods=["POST"])
def assign_driver(mongo_id):
    """
    Assign a driver.
    Body: { "driver_id": "<driver mongo _id as string>" }
    """
    body = request.json or {}
    driver_id_str = body.get("driver_id")
    if not driver_id_str:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    try:
        order_oid = ObjectId(mongo_id)
        driver_oid = ObjectId(driver_id_str)
    except Exception:
        return jsonify({"ok": False, "error": "bad ObjectId"}), 400

    db.orders.update_one(
        {"_id": order_oid},
        {"$set": {
            "assigned_driver_id": driver_oid,
            "assigned_at": datetime.utcnow(),
            "status": "assigned"
        }}
    )

    log_audit("orders", order_oid, "assign_driver", {
        "driver_id": driver_id_str
    })

    return jsonify({"ok": True}), 200


@app.route("/orders/<mongo_id>/status", methods=["POST"])
def update_status(mongo_id):
    """
    Update status.
    Body: { "status": "pending"|"assigned"|"in_transit"|"delivered"|"cancelled"|"failed"|"review_required" }
    """
    body = request.json or {}
    new_status = body.get("status")
    allowed = [
        "pending",
        "assigned",
        "in_transit",
        "delivered",
        "cancelled",
        "failed",
        "review_required"
    ]
    if new_status not in allowed:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    try:
        order_oid = ObjectId(mongo_id)
    except Exception:
        return jsonify({"ok": False, "error": "bad ObjectId"}), 400

    update_set = {"status": new_status}
    if new_status == "delivered":
        update_set["delivered_at"] = datetime.utcnow()

    db.orders.update_one(
        {"_id": order_oid},
        {"$set": update_set}
    )

    log_audit("orders", order_oid, "status_change", {
        "status": new_status
    })

    return jsonify({"ok": True}), 200


@app.route("/drivers", methods=["POST"])
def create_driver():
    """
    Driver signup from the modal.
    Body:
    {
      "driver_id": "DRV-123456",
      "name": "Sibongile",
      "phone": "2782...",
      "vehicle": "car" | "bike" | etc,
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

    try:
        res = db.drivers.insert_one(driver_doc)
    except Exception as e:
        print("driver insert failed:", e)
        return jsonify({"ok": False, "error": "db_insert_failed", "details": str(e)}), 500

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


# Local dev runner (so you can `python app.py` on your laptop)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
