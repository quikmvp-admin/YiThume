import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

# -------------------------------------------------
# Simple in-memory "database"
# (THIS WILL RESET every time the function cold-starts)
# -------------------------------------------------

ORDERS = []   # list of dicts
DRIVERS = []  # list of dicts


# -------------------------------------------------
# Flask app
# -------------------------------------------------

app = Flask(__name__)
CORS(app)


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def make_order_public_id():
    # Example: YI-20251027-AB12CD
    ts = datetime.utcnow().strftime("%Y%m%d")
    short = str(uuid.uuid4())[:6].upper()
    return f"YI-{ts}-{short}"

def find_order_by_internal_id(internal_id):
    for o in ORDERS:
        if o.get("_internal_id") == internal_id:
            return o
    return None

def find_driver_by_internal_id(internal_id):
    for d in DRIVERS:
        if d.get("_internal_id") == internal_id:
            return d
    return None


# -------------------------------------------------
# Routes
#
# NOTE:
# Your frontend calls /api/app/... (BASE_API = "/api/app")
# The vercel.json will route /api/app... -> this file.
# We expose each route with BOTH:
#   1. /api/app/...   (prod on Vercel)
#   2. /...           (local testing if you run `flask run`)
# -------------------------------------------------


# Healthcheck
@app.route("/", methods=["GET"])
@app.route("/api/app", methods=["GET"])
def healthcheck():
    return jsonify({
        "ok": True,
        "service": "YiThume backend (in-memory demo)",
        "orders_count": len(ORDERS),
        "drivers_count": len(DRIVERS)
    }), 200


# ---------------------------
# ORDERS
# ---------------------------

@app.route("/orders", methods=["POST"])
@app.route("/api/app/orders", methods=["POST"])
def create_order():
    """
    Create a new order from the website checkout modal.
    This matches what your frontend sends in orderPayload.
    """
    data = request.json or {}

    # Build the order object
    order_public_id = make_order_public_id()
    internal_id = str(uuid.uuid4())  # internal reference

    order_doc = {
        "_internal_id": internal_id,          # only for server use
        "order_id": order_public_id,          # public ref shown to user
        "created_at": datetime.utcnow().isoformat() + "Z",

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

        # order state
        "status": "pending",
        "assigned_driver_id": None,
        "assigned_at": None,
        "delivered_at": None,

        # logistics-ish info
        "route": data.get("route", {}),

        # simple "fraud" placeholders so your UI doesn't break
        "fraud_score": 0.0,
        "fraud_flags": {},

        "created_by": data.get("created_by", "web"),
        "meta": data.get("meta", {})
    }

    # Very basic "review_required" rule:
    # if total is huge, mark for review
    if order_doc["total"] >= 500:
        order_doc["status"] = "review_required"
        order_doc["fraud_score"] = 0.8
        order_doc["fraud_flags"] = {"high_value": True}

    # Save to memory
    ORDERS.insert(0, order_doc)  # put newest first

    # Respond like the old Mongo version so frontend still works
    return jsonify({
        "ok": True,
        "order_db_id": internal_id,
        "order_public_id": order_public_id,
        "status": order_doc["status"],
        "fraud_score": order_doc["fraud_score"],
        "fraud_flags": order_doc["fraud_flags"]
    }), 201


@app.route("/orders", methods=["GET"])
@app.route("/api/app/orders", methods=["GET"])
def list_orders():
    """
    Admin panel calls GET /api/app/orders?status=pending etc.
    We'll filter by status if provided.
    """
    status_filter = request.args.get("status")
    if status_filter:
        filtered = [o for o in ORDERS if o.get("status") == status_filter]
    else:
        filtered = ORDERS

    # Return a copy without internal-only field
    out = []
    for o in filtered:
        safe = dict(o)
        # internal id is still useful to call /assign and /status
        # but we won't expose it as _internal_id in UI, so leave it.
        out.append(safe)

    return jsonify({
        "ok": True,
        "orders": out
    }), 200


@app.route("/orders/<oid>/assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/assign", methods=["POST"])
def assign_driver(oid):
    """
    Admin 'Assign' would POST here with {"driver_id": "..."}.
    oid is _internal_id from the order.
    """
    body = request.json or {}
    driver_id = body.get("driver_id")

    if not driver_id:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    order_doc = find_order_by_internal_id(oid)
    if not order_doc:
        return jsonify({"ok": False, "error": "order not found"}), 404

    drv_doc = find_driver_by_internal_id(driver_id)
    if not drv_doc:
        return jsonify({"ok": False, "error": "driver not found"}), 404

    order_doc["assigned_driver_id"] = driver_id
    order_doc["assigned_at"] = datetime.utcnow().isoformat() + "Z"
    order_doc["status"] = "assigned"

    return jsonify({"ok": True}), 200


@app.route("/orders/<oid>/status", methods=["POST"])
@app.route("/api/app/orders/<oid>/status", methods=["POST"])
def update_status(oid):
    """
    Update order status.
    Body: {"status": "delivered"} etc.
    """
    body = request.json or {}
    new_status = body.get("status")

    allowed_status = [
        "pending", "assigned", "in_transit",
        "delivered", "cancelled", "failed",
        "review_required"
    ]
    if new_status not in allowed_status:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    order_doc = find_order_by_internal_id(oid)
    if not order_doc:
        return jsonify({"ok": False, "error": "order not found"}), 404

    order_doc["status"] = new_status
    if new_status == "delivered":
        order_doc["delivered_at"] = datetime.utcnow().isoformat() + "Z"

    return jsonify({"ok": True}), 200


# ---------------------------
# DRIVERS
# ---------------------------

@app.route("/drivers", methods=["POST"])
@app.route("/api/app/drivers", methods=["POST"])
def create_driver():
    """
    Driver signup modal calls POST /api/app/drivers
    with { name, phone, vehicle, available, current_location{lat,lng}, meta{zone,radius_km} }
    We'll store in memory and return an ID.
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
            "updated_at": datetime.utcnow().isoformat() + "Z"
        },
        "weekly_payout_due": 0.0,
        "earnings_history": [],
        "ratings": {"count": 0, "avg": None},
        "meta": data.get("meta", {})
    }

    DRIVERS.insert(0, driver_doc)

    return jsonify({
        "ok": True,
        "driver_db_id": internal_id
    }), 201


@app.route("/drivers", methods=["GET"])
@app.route("/api/app/drivers", methods=["GET"])
def list_drivers():
    """
    Return all active drivers.
    """
    out = []
    for d in DRIVERS:
        out.append(dict(d))
    return jsonify({"ok": True, "drivers": out}), 200


# NOTE:
# DO NOT add `if __name__ == "__main__": app.run(...)`
# Vercel imports this file and serves `app` automatically.
