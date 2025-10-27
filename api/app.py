import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

ORDERS = []
DRIVERS = []

app = Flask(__name__)
CORS(app)

def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

def make_order_public_id():
    ts = datetime.utcnow().strftime("%Y%m%d")
    return f"YI-{ts}-{str(uuid.uuid4())[:6].upper()}"

def _find(lst, key, value):
    for x in lst:
        if x.get(key) == value:
            return x
    return None

@app.route("/", methods=["GET"])
@app.route("/api/app", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "service": "YiThume (in-memory)",
        "orders_count": len(ORDERS),
        "drivers_count": len(DRIVERS)
    }), 200

@app.route("/orders", methods=["POST"])
@app.route("/api/app/orders", methods=["POST"])
def create_order():
    data = request.json or {}

    internal_id = str(uuid.uuid4())
    public_id   = make_order_public_id()
    total       = data.get("total", 0)

    order = {
        "_internal_id": internal_id,
        "order_id": public_id,
        "created_at": _now_iso(),
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

    if total >= 500:
        order["status"] = "review_required"
        order["fraud_score"] = 0.8
        order["fraud_flags"] = {"high_value": True}

    ORDERS.insert(0, order)

    return jsonify({
        "ok": True,
        "order_db_id": internal_id,
        "order_public_id": public_id,
        "status": order["status"],
        "fraud_score": order["fraud_score"],
        "fraud_flags": order["fraud_flags"]
    }), 201

@app.route("/orders", methods=["GET"])
@app.route("/api/app/orders", methods=["GET"])
def list_orders():
    status = request.args.get("status")
    rows = [o for o in ORDERS if (o["status"] == status)] if status else ORDERS
    return jsonify({"ok": True, "orders": rows}), 200

@app.route("/orders/<oid>/assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/assign", methods=["POST"])
def assign_driver(oid):
    body = request.json or {}
    driver_id = body.get("driver_id")
    if not driver_id:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    order = _find(ORDERS, "_internal_id", oid)
    if not order:
        return jsonify({"ok": False, "error": "order not found"}), 404

    drv = _find(DRIVERS, "_internal_id", driver_id)
    if not drv:
        return jsonify({"ok": False, "error": "driver not found"}), 404

    order["assigned_driver_id"] = driver_id
    order["assigned_at"] = _now_iso()
    order["status"] = "assigned"
    return jsonify({"ok": True}), 200

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

    order = _find(ORDERS, "_internal_id", oid)
    if not order:
        return jsonify({"ok": False, "error": "order not found"}), 404

    order["status"] = new_status
    if new_status == "delivered":
        order["delivered_at"] = _now_iso()

    return jsonify({"ok": True}), 200

@app.route("/drivers", methods=["POST"])
@app.route("/api/app/drivers", methods=["POST"])
def create_driver():
    data = request.json or {}
    internal_id = str(uuid.uuid4())

    driver = {
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
            "updated_at": _now_iso()
        },
        "weekly_payout_due": 0.0,
        "earnings_history": [],
        "ratings": {"count": 0, "avg": None},
        "meta": data.get("meta", {})
    }

    DRIVERS.insert(0, driver)

    return jsonify({
        "ok": True,
        "driver_db_id": internal_id
    }), 201

@app.route("/drivers", methods=["GET"])
@app.route("/api/app/drivers", methods=["GET"])
def list_drivers():
    return jsonify({"ok": True, "drivers": DRIVERS}), 200

# no app.run(); Vercel imports `app`
