import os
import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient, errors as mongo_errors

# -------------------------------------------------
# ENV + LAZY MONGO
# -------------------------------------------------

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)

# We'll create the client up front, but we won't assume it's valid until we try.
mongo_client = MongoClient(MONGO_URI)
DB_NAME = "yithume"

def get_db():
    """
    Return a live db handle if Mongo works.
    If Mongo is down / bad URI / not allowed, raise RuntimeError.
    """
    try:
        # cheap ping to ensure connection works
        mongo_client.admin.command("ping")
        return mongo_client[DB_NAME]
    except Exception as e:
        raise RuntimeError(f"Mongo connection failed: {e}")


# -------------------------------------------------
# FLASK APP
# -------------------------------------------------

app = Flask(__name__)
CORS(app)

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def _now_dt():
    return datetime.utcnow()

def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

def make_order_public_id():
    ts = datetime.utcnow().strftime("%Y%m%d")
    return f"YI-{ts}-{str(uuid.uuid4())[:6].upper()}"

def safe_order_doc(doc):
    if not doc:
        return None
    out = dict(doc)
    out.pop("_id", None)

    # normalize datetimes
    if isinstance(out.get("created_at"), datetime):
        out["created_at"] = out["created_at"].isoformat() + "Z"
    if isinstance(out.get("assigned_at"), datetime):
        out["assigned_at"] = out["assigned_at"].isoformat() + "Z"
    if isinstance(out.get("delivered_at"), datetime):
        out["delivered_at"] = out["delivered_at"].isoformat() + "Z"

    return out

def safe_driver_doc(doc):
    if not doc:
        return None
    out = dict(doc)
    out.pop("_id", None)

    loc = out.get("current_location")
    if loc and isinstance(loc.get("updated_at"), datetime):
        loc["updated_at"] = loc["updated_at"].isoformat() + "Z"

    return out


# -------------------------------------------------
# ROUTES
# (we expose both /api/app/... for production and /... for local dev)
# -------------------------------------------------

@app.route("/", methods=["GET"])
@app.route("/api/app", methods=["GET"])
def health():
    try:
        db = get_db()
        orders_count = db.orders.count_documents({})
        drivers_count = db.drivers.count_documents({"active": True})
        return jsonify({
            "ok": True,
            "service": "YiThume (mongo)",
            "db": "up",
            "orders_count": orders_count,
            "drivers_count": drivers_count
        }), 200
    except RuntimeError as e:
        # DB not reachable
        return jsonify({
            "ok": True,
            "service": "YiThume (mongo)",
            "db": "down",
            "error": str(e)
        }), 200


# ---------------------------
# CREATE ORDER
# ---------------------------
@app.route("/orders", methods=["POST"])
@app.route("/api/app/orders", methods=["POST"])
def create_order():
    data = request.json or {}

    internal_id = str(uuid.uuid4())          # server-UUID
    public_id   = make_order_public_id()     # human ref
    total       = data.get("total", 0)

    order_doc = {
        "_internal_id": internal_id,
        "order_id": public_id,
        "created_at": _now_dt(),
        "created_at_iso": _now_iso(),
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

    # simple "review_required" rule
    if total >= 500:
        order_doc["status"] = "review_required"
        order_doc["fraud_score"] = 0.8
        order_doc["fraud_flags"] = {"high_value": True}

    try:
        db = get_db()
        db.orders.insert_one(order_doc)
    except RuntimeError as e:
        # Mongo unreachable
        return jsonify({
            "ok": False,
            "error": "db_unavailable",
            "details": str(e)
        }), 500
    except mongo_errors.PyMongoError as e:
        # Driver blew up somewhere else
        return jsonify({
            "ok": False,
            "error": "db_write_failed",
            "details": str(e)
        }), 500

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

    try:
        db = get_db()
        cursor = (
            db.orders
              .find(query)
              .sort("created_at", -1)
              .limit(50)
        )
        out = [safe_order_doc(o) for o in cursor]
        return jsonify({"ok": True, "orders": out}), 200

    except RuntimeError as e:
        return jsonify({
            "ok": False,
            "error": "db_unavailable",
            "details": str(e),
            "orders": []
        }), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({
            "ok": False,
            "error": "db_read_failed",
            "details": str(e),
            "orders": []
        }), 500


# ---------------------------
# ASSIGN DRIVER
# ---------------------------
@app.route("/orders/<oid>/assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/assign", methods=["POST"])
def assign_driver(oid):
    body = request.json or {}
    driver_internal_id = body.get("driver_id")
    if not driver_internal_id:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    try:
        db = get_db()

        order_doc = db.orders.find_one({"_internal_id": oid})
        if not order_doc:
            return jsonify({"ok": False, "error": "order not found"}), 404

        driver_doc = db.drivers.find_one({
            "_internal_id": driver_internal_id,
            "active": True
        })
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

    except RuntimeError as e:
        return jsonify({
            "ok": False,
            "error": "db_unavailable",
            "details": str(e)
        }), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({
            "ok": False,
            "error": "db_write_failed",
            "details": str(e)
        }), 500


# ---------------------------
# UPDATE ORDER STATUS
# ---------------------------
@app.route("/orders/<oid>/status", methods=["POST"])
@app.route("/api/app/orders/<oid>/status", methods=["POST"])
def update_status(oid):
    body = request.json or {}
    new_status = body.get("status")

    allowed = {
        "pending","assigned","in_transit",
        "delivered","cancelled","failed",
        "review_required"
    }
    if new_status not in allowed:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    update_fields = {"status": new_status}
    if new_status == "delivered":
        update_fields["delivered_at"] = _now_dt()

    try:
        db = get_db()

        order_doc = db.orders.find_one({"_internal_id": oid})
        if not order_doc:
            return jsonify({"ok": False, "error": "order not found"}), 404

        db.orders.update_one(
            {"_internal_id": oid},
            {"$set": update_fields}
        )

        return jsonify({"ok": True}), 200

    except RuntimeError as e:
        return jsonify({
            "ok": False,
            "error": "db_unavailable",
            "details": str(e)
        }), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({
            "ok": False,
            "error": "db_write_failed",
            "details": str(e)
        }), 500


# ---------------------------
# CREATE DRIVER
# ---------------------------
@app.route("/drivers", methods=["POST"])
@app.route("/api/app/drivers", methods=["POST"])
def create_driver():
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

    try:
        db = get_db()
        db.drivers.insert_one(driver_doc)

        return jsonify({
            "ok": True,
            "driver_db_id": internal_id
        }), 201

    except RuntimeError as e:
        return jsonify({
            "ok": False,
            "error": "db_unavailable",
            "details": str(e)
        }), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({
            "ok": False,
            "error": "db_write_failed",
            "details": str(e)
        }), 500


# ---------------------------
# LIST DRIVERS
# ---------------------------
@app.route("/drivers", methods=["GET"])
@app.route("/api/app/drivers", methods=["GET"])
def list_drivers():
    try:
        db = get_db()
        cursor = db.drivers.find({"active": True})
        out = [safe_driver_doc(d) for d in cursor]
        return jsonify({"ok": True, "drivers": out}), 200

    except RuntimeError as e:
        return jsonify({
            "ok": False,
            "error": "db_unavailable",
            "details": str(e),
            "drivers": []
        }), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({
            "ok": False,
            "error": "db_read_failed",
            "details": str(e),
            "drivers": []
        }), 500

# no app.run(); Vercel imports `app`
