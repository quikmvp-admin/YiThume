import os
import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient

# -------------------------------------------------
# Mongo connection
# -------------------------------------------------
#
# On Vercel:
#   Project Settings → Environment Variables
#   Name: MONGO_URI
#   Value: your full mongodb+srv://... string
#
# We default to a placeholder so local dev doesn't instantly die.
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)

client = MongoClient(MONGO_URI)
db = client["yithume"]  # DB name in Atlas


# -------------------------------------------------
# Flask app
# -------------------------------------------------
app = Flask(__name__)
CORS(app)


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def _now_dt():
    return datetime.utcnow()

def _now_iso():
    # We'll store a machine datetime for sorting and a string for display
    return datetime.utcnow().isoformat() + "Z"

def make_order_public_id():
    ts = datetime.utcnow().strftime("%Y%m%d")
    return f"YI-{ts}-{str(uuid.uuid4())[:6].upper()}"

def safe_order_doc(doc):
    """
    Convert Mongo order doc to something safe for frontend.
    - Drop Mongo _id (ObjectId)
    - Convert datetimes to strings
    """
    if not doc:
        return None
    out = dict(doc)
    out.pop("_id", None)

    # normalize created_at for UI
    if "created_at" in out and isinstance(out["created_at"], datetime):
        out["created_at"] = out["created_at"].isoformat() + "Z"

    # we already store created_at_iso separately anyway
    return out

def safe_driver_doc(doc):
    if not doc:
        return None
    out = dict(doc)
    out.pop("_id", None)

    # normalize current_location.updated_at
    loc = out.get("current_location")
    if loc and isinstance(loc.get("updated_at"), datetime):
        loc["updated_at"] = loc["updated_at"].isoformat() + "Z"

    return out


# -------------------------------------------------
# ROUTES
#   NOTE:
#   Your frontend calls /api/app/...
#   We also expose bare /... for local `flask run` testing.
# -------------------------------------------------


@app.route("/", methods=["GET"])
@app.route("/api/app", methods=["GET"])
def health():
    orders_count = db.orders.count_documents({})
    drivers_count = db.drivers.count_documents({"active": True})

    return jsonify({
        "ok": True,
        "service": "YiThume (mongo)",
        "orders_count": orders_count,
        "drivers_count": drivers_count
    }), 200


# ---------------------------
# CREATE ORDER
# ---------------------------
@app.route("/orders", methods=["POST"])
@app.route("/api/app/orders", methods=["POST"])
def create_order():
    data = request.json or {}

    internal_id = str(uuid.uuid4())          # server-side primary ref
    public_id   = make_order_public_id()     # human-friendly Ref
    total       = data.get("total", 0)

    order_doc = {
        "_internal_id": internal_id,         # used by admin panel actions
        "order_id": public_id,               # shown to customer
        "created_at": _now_dt(),             # datetime for sorting in Mongo
        "created_at_iso": _now_iso(),        # human readable string
        "customer": data.get("customer", {}),
        "items": data.get("items", []),
        "subtotal": data.get("subtotal", 0),
        "delivery_fee": data.get("delivery_fee", 0),
        "total": total,
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

    # basic review rule so UI still shows "review_required"
    if total >= 500:
        order_doc["status"] = "review_required"
        order_doc["fraud_score"] = 0.8
        order_doc["fraud_flags"] = {"high_value": True}

    # insert into Mongo
    db.orders.insert_one(order_doc)

    # response shape matches what your frontend expects
    return jsonify({
        "ok": True,
        "order_db_id": internal_id,
        "order_public_id": public_id,
        "status": order_doc["status"],
        "fraud_score": order_doc["fraud_score"],
        "fraud_flags": order_doc["fraud_flags"]
    }), 201


# ---------------------------
# LIST ORDERS (ADMIN PANEL)
# ---------------------------
@app.route("/orders", methods=["GET"])
@app.route("/api/app/orders", methods=["GET"])
def list_orders():
    status = request.args.get("status")

    query = {}
    if status:
        query["status"] = status

    # newest first
    cursor = (
        db.orders
          .find(query)
          .sort("created_at", -1)
          .limit(50)
    )

    out = [safe_order_doc(o) for o in cursor]
    return jsonify({"ok": True, "orders": out}), 200


# ---------------------------
# ASSIGN DRIVER TO ORDER
# ---------------------------
@app.route("/orders/<oid>/assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/assign", methods=["POST"])
def assign_driver(oid):
    """
    oid here is _internal_id for an order (not Mongo _id)
    body.driver_id should be the driver's _internal_id
    """
    body = request.json or {}
    driver_internal_id = body.get("driver_id")
    if not driver_internal_id:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    order_doc = db.orders.find_one({"_internal_id": oid})
    if not order_doc:
        return jsonify({"ok": False, "error": "order not found"}), 404

    driver_doc = db.drivers.find_one({"_internal_id": driver_internal_id, "active": True})
    if not driver_doc:
        return jsonify({"ok": False, "error": "driver not found"}), 404

    db.orders.update_one(
        {"_internal_id": oid},
        {"$set": {
            "assigned_driver_id": driver_internal_id,
            "assigned_at": _now_dt(),
            "status": "assigned"
        }}
    )

    return jsonify({"ok": True}), 200


# ---------------------------
# UPDATE ORDER STATUS
# ---------------------------
@app.route("/orders/<oid>/status", methods=["POST"])
@app.route("/api/app/orders/<oid>/status", methods=["POST"])
def update_status(oid):
    """
    Allows moving an order to in_transit / delivered / etc.
    oid is _internal_id.
    """
    body = request.json or {}
    new_status = body.get("status")

    allowed = {
        "pending","assigned","in_transit",
        "delivered","cancelled","failed",
        "review_required"
    }
    if new_status not in allowed:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    order_doc = db.orders.find_one({"_internal_id": oid})
    if not order_doc:
        return jsonify({"ok": False, "error": "order not found"}), 404

    update_fields = {"status": new_status}
    if new_status == "delivered":
        update_fields["delivered_at"] = _now_dt()

    db.orders.update_one(
        {"_internal_id": oid},
        {"$set": update_fields}
    )

    return jsonify({"ok": True}), 200


# ---------------------------
# CREATE DRIVER
# ---------------------------
@app.route("/drivers", methods=["POST"])
@app.route("/api/app/drivers", methods=["POST"])
def create_driver():
    """
    Driver signup modal posts:
      {
        name, phone, vehicle, available, current_location: {lat,lng},
        meta: { zone, radius_km }
      }
    We assign an internal id for that driver and save them.
    """
    data = request.json or {}
    internal_id = str(uuid.uuid4())

    driver_doc = {
        "_internal_id": internal_id,
        "driver_id": data.get("driver_id") or f"DRV-{internal_id[:6].upper()}",
        "name": data.get("name"),
        "phone": data.get("phone"),
        "vehicle": data.get("vehicle", "car"),
        "active": True,
        "available": data.get("available", True),
        "current_location": {
            "lat": (data.get("current_location") or {}).get("lat"),
            "lng": (data.get("current_location") or {}).get("lng"),
            "updated_at": _now_dt()
        },
        "weekly_payout_due": 0.0,
        "earnings_history": [],
        "ratings": {"count": 0, "avg": None},
        "meta": data.get("meta", {})
    }

    db.drivers.insert_one(driver_doc)

    return jsonify({
        "ok": True,
        "driver_db_id": internal_id
    }), 201


# ---------------------------
# LIST DRIVERS
# ---------------------------
@app.route("/drivers", methods=["GET"])
@app.route("/api/app/drivers", methods=["GET"])
def list_drivers():
    cursor = db.drivers.find({"active": True})
    out = [safe_driver_doc(d) for d in cursor]
    return jsonify({"ok": True, "drivers": out}), 200


