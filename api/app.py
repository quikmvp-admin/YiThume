import os
import re
import uuid
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt

from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING, DESCENDING, errors as mongo_errors

# -------------------------------------------------
# ENV + LAZY MONGO
# -------------------------------------------------
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)
mongo_client = MongoClient(MONGO_URI)
DB_NAME = "yithume"

def get_db():
    try:
        mongo_client.admin.command("ping")
        return mongo_client[DB_NAME]
    except Exception as e:
        raise RuntimeError(f"Mongo connection failed: {e}")

# -------------------------------------------------
# CONFIG / BUSINESS CONSTANTS
# -------------------------------------------------
ITEM_MARGIN_RATE = 0.12      # 12% margin if item.cost not provided
PLATFORM_FEE_RATE = 0.10     # 10% of delivery fee goes to platform (base)
BATCH_BONUS_PER_EXTRA = 0.25 # 25% of delivery fee for each extra drop in same cluster
BATCH_BONUS_CAP = 0.60       # max 60% total bonus on batched run
CLUSTER_WINDOW_MIN = 120     # cluster orders if same area/window within 120min
AUTO_ASSIGN_RADIUS_KM = 12   # max distance for nearby driver
SERVICE_BBOX = {             # very rough EC bounding box
    "min_lat": -34.2, "max_lat": -33.0,
    "min_lng":  25.5, "max_lng":  27.5
}

# -------------------------------------------------
# FLASK
# -------------------------------------------------
app = Flask(__name__)
CORS(app)

# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def _now_dt(): return datetime.utcnow()
def _now_iso(): return _now_dt().isoformat() + "Z"

def make_order_public_id():
    ts = datetime.utcnow().strftime("%Y%m%d")
    return f"YI-{ts}-{str(uuid.uuid4())[:6].upper()}"

def safe_doc(doc):
    if not doc: return None
    out = dict(doc); out.pop("_id", None)
    for k in ("created_at","assigned_at","delivered_at"):
        if isinstance(out.get(k), datetime):
            out[k] = out[k].isoformat() + "Z"
    loc = out.get("current_location")
    if loc and isinstance(loc.get("updated_at"), datetime):
        loc["updated_at"] = loc["updated_at"].isoformat() + "Z"
    return out

def phone_ok(p):
    return bool(re.fullmatch(r"\d{10,15}", str(p or "").strip()))

def inside_service_area(lat, lng):
    if lat is None or lng is None:  # don't block if missing
        return True
    bb = SERVICE_BBOX
    return (bb["min_lat"] <= lat <= bb["max_lat"]) and (bb["min_lng"] <= lng <= bb["max_lng"])

def haversine_km(lat1, lon1, lat2, lon2):
    # distance between 2 lat/lng in KM
    if None in (lat1, lon1, lat2, lon2): return None
    r = 6371.0
    dlat, dlon = radians(lat2-lat1), radians(lon2-lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2*r*asin(sqrt(a))

def ensure_indexes(db):
    db.orders.create_index([("created_at", DESCENDING)])
    db.orders.create_index([("_internal_id", ASCENDING)], unique=True)
    db.orders.create_index([("customer.phone", ASCENDING), ("created_at", DESCENDING)])
    db.orders.create_index([("status", ASCENDING), ("created_at", DESCENDING)])
    db.drivers.create_index([("_internal_id", ASCENDING)], unique=True)
    db.drivers.create_index([("active", ASCENDING), ("available", ASCENDING), ("meta.zone", ASCENDING)])

def wa_order_text(order):
    items_list = ", ".join([f"{i.get('name')} x{i.get('qty')}" for i in order.get("items", [])])
    addr = order.get("customer", {}).get("address", {})
    eta = order.get("route", {}).get("eta_text", "TBC")
    total = order.get("total", 0)
    zone  = order.get("meta", {}).get("zone", "")
    lines = [
        "Yi Thume, my order is:",
        f"Ref: {order.get('order_id')}",
        f"Items: {items_list}",
        f"Deliver to: {addr.get('line1','')} (Zone {zone})",
        f"Total: R{total}",
        f"Expected delivery: {eta}",
        "Please confirm and send payment link."
    ]
    return "\n".join(lines)

# ---------------- Fraud & Legitimacy (lightweight rules) ---------------
def rule_based_fraud_score(db, order_doc):
    score = 0.0
    flags = {}

    phone = (order_doc.get("customer") or {}).get("phone")
    if not phone_ok(phone):
        flags["bad_phone"] = True; score += 0.2

    # phone velocity last 60m
    recent_count = db.orders.count_documents({
        "customer.phone": phone,
        "created_at": {"$gte": _now_dt() - timedelta(minutes=60)}
    }) if phone else 0
    if recent_count >= 3:
        flags["phone_velocity"] = True; score += 0.4

    # duplicate subtotal in last 10m
    if phone:
        dup = db.orders.find_one({
            "customer.phone": phone,
            "subtotal": order_doc.get("subtotal", 0),
            "created_at": {"$gte": _now_dt() - timedelta(minutes=10)}
        })
        if dup:
            flags["duplicate_like"] = True; score += 0.3

    # rough out-of-area
    coords = (((order_doc.get("customer") or {}).get("address") or {}).get("coords") or {})
    if not inside_service_area(coords.get("lat"), coords.get("lng")):
        flags["out_of_area"] = True; score += 0.5

    # high value vs rolling avg
    pipeline = [{"$group": {"_id": None, "avg": {"$avg": "$total"}}}]
    agg = list(db.orders.aggregate(pipeline))
    avg_total = agg[0]["avg"] if agg else 50
    if order_doc.get("total", 0) > avg_total * 3:
        flags["high_value"] = True; score += 0.2

    return min(score, 1.0), flags

# ---------------- Driver availability / matching -----------------------
def find_available_driver(db, zone, drop_lat=None, drop_lng=None):
    # Prefer by zone, then by proximity if coords provided
    q = {"active": True, "available": True}
    if zone: q["meta.zone"] = zone
    cur = list(db.drivers.find(q))
    if not cur: return None

    # If no coords, return first
    if drop_lat is None or drop_lng is None:
        return cur[0]

    # rank by distance
    best = None; best_d = 1e9
    for d in cur:
        loc = (d.get("current_location") or {})
        km = haversine_km(drop_lat, drop_lng, loc.get("lat"), loc.get("lng"))
        if km is None: continue
        if km <= AUTO_ASSIGN_RADIUS_KM and km < best_d:
            best = d; best_d = km
    return best or cur[0]

def cluster_key(order_doc):
    # cluster by (zone + coarse address + 2h window)
    addr  = ((order_doc.get("customer") or {}).get("address") or {})
    zone  = (order_doc.get("meta") or {}).get("zone", "")
    line1 = (addr.get("line1") or "").strip().lower()
    # take first word/landmark as coarse anchor
    coarse = re.split(r"[,\s]+", line1)[0] if line1 else "unknown"
    bucket = (_now_dt().replace(minute=0, second=0, microsecond=0) // timedelta(minutes=CLUSTER_WINDOW_MIN))
    return f"{zone}:{coarse}:{bucket}"

# ---------------- Earnings & Settlement --------------------------------
def compute_earnings(order_doc, prior_in_cluster=0):
    fee = float(order_doc.get("delivery_fee", 0))
    # platform gets a base %
    platform_cut = fee * PLATFORM_FEE_RATE
    driver_cut   = fee - platform_cut

    # batching bonus: each extra drop adds 25% of fee up to 60% cap
    if prior_in_cluster > 0:
        bonus = min(prior_in_cluster * BATCH_BONUS_PER_EXTRA, BATCH_BONUS_CAP) * fee
        driver_cut += bonus
        platform_cut = max(0.0, fee - driver_cut)

    # margin on items (if item.cost present use it; else apply a small % on price)
    items = order_doc.get("items", [])
    margin = 0.0
    for it in items:
        price = float(it.get("price", 0))
        cost  = it.get("cost")
        if cost is not None:
            margin += max(0.0, price - float(cost)) * int(it.get("qty", 1))
        else:
            margin += (price * ITEM_MARGIN_RATE) * int(it.get("qty", 1))

    platform_total = platform_cut + margin
    return round(driver_cut, 2), round(platform_total, 2)

def accrue_driver_earning(db, driver_internal_id, amount, reason, order_id):
    db.drivers.update_one(
        {"_internal_id": driver_internal_id},
        {"$inc": {"weekly_payout_due": amount},
         "$push": {"earnings_history": {
             "amount": amount, "reason": reason, "order_id": order_id, "ts": _now_dt()
         }}}
    )

# -------------------------------------------------
# STARTUP: create indexes
# -------------------------------------------------
try:
    ensure_indexes(get_db())
except Exception:
    # ignore on cold start without DB
    pass

# -------------------------------------------------
# ROUTES
# -------------------------------------------------
@app.route("/", methods=["GET"])
@app.route("/api/app", methods=["GET"])
def health():
    try:
        db = get_db()
        orders_count = db.orders.count_documents({})
        drivers_count = db.drivers.count_documents({"active": True})
        return jsonify({"ok": True, "service": "YiThume (mongo+logic)", "db": "up",
                        "orders_count": orders_count, "drivers_count": drivers_count}), 200
    except RuntimeError as e:
        return jsonify({"ok": True, "service": "YiThume (mongo+logic)", "db": "down", "error": str(e)}), 200

# ---------------- CREATE ORDER (gated by driver availability) -----------
@app.route("/orders", methods=["POST"])
@app.route("/api/app/orders", methods=["POST"])
def create_order():
    data = request.json or {}

    internal_id = str(uuid.uuid4())
    public_id   = make_order_public_id()
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
        "payment": data.get("payment", {"method": "cash","status": "pending","provider_ref": None}),
        "status": "pending",    # becomes 'assigned' after payment/auto-assign
        "assigned_driver_id": None,
        "assigned_at": None,
        "delivered_at": None,
        "route": data.get("route", {}),
        "created_by": data.get("created_by", "web"),
        "meta": data.get("meta", {}),

        # fraud / settlement fields
        "fraud_score": 0.0,
        "fraud_flags": {},
        "cluster_key": None,
        "settlement": {"driver": 0.0, "platform": 0.0, "settled": False}
    }

    try:
        db = get_db()

        # gate: check driver availability in zone (and roughly near coords)
        zone = (order_doc["meta"] or {}).get("zone")
        coords = (((order_doc.get("customer") or {}).get("address") or {}).get("coords") or {})
        candidate = find_available_driver(db, zone, coords.get("lat"), coords.get("lng"))
        if not candidate:
            return jsonify({"ok": False, "error": "no_driver_available"}), 409

        # fraud score
        fs, ff = rule_based_fraud_score(db, order_doc)
        order_doc["fraud_score"], order_doc["fraud_flags"] = fs, ff
        if fs >= 0.75:
            order_doc["status"] = "review_required"

        # cluster key (for batching economics later)
        order_doc["cluster_key"] = cluster_key(order_doc)

        db.orders.insert_one(order_doc)

        # include prebuilt WA text for your UI
        return jsonify({
            "ok": True,
            "order_db_id": internal_id,
            "order_public_id": public_id,
            "status": order_doc["status"],
            "fraud_score": fs,
            "fraud_flags": ff,
            "wa_message": wa_order_text(order_doc)
        }), 201

    except RuntimeError as e:
        return jsonify({"ok": False, "error": "db_unavailable", "details": str(e)}), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({"ok": False, "error": "db_write_failed", "details": str(e)}), 500

# ---------------- LIST ORDERS ------------------------------------------
@app.route("/orders", methods=["GET"])
@app.route("/api/app/orders", methods=["GET"])
def list_orders():
    status = request.args.get("status")
    q = {"status": status} if status else {}
    try:
        db = get_db()
        cur = db.orders.find(q).sort("created_at", DESCENDING).limit(50)
        return jsonify({"ok": True, "orders": [safe_doc(o) for o in cur]}), 200
    except RuntimeError as e:
        return jsonify({"ok": False, "error": "db_unavailable", "details": str(e), "orders": []}), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({"ok": False, "error": "db_read_failed", "details": str(e), "orders": []}), 500

# ---------------- AUTO-ASSIGN (best nearby driver) ---------------------
@app.route("/orders/<oid>/auto-assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/auto-assign", methods=["POST"])
def auto_assign(oid):
    try:
        db = get_db()
        o = db.orders.find_one({"_internal_id": oid})
        if not o: return jsonify({"ok": False, "error": "order not found"}), 404

        zone = (o.get("meta") or {}).get("zone")
        coords = (((o.get("customer") or {}).get("address") or {}).get("coords") or {})
        d = find_available_driver(db, zone, coords.get("lat"), coords.get("lng"))
        if not d: return jsonify({"ok": False, "error": "no_driver_available"}), 409

        db.orders.update_one({"_internal_id": oid}, {"$set": {
            "assigned_driver_id": d["_internal_id"], "assigned_at": _now_dt(), "status": "assigned"
        }})
        return jsonify({"ok": True, "driver_id": d["_internal_id"]}), 200

    except RuntimeError as e:
        return jsonify({"ok": False, "error": "db_unavailable", "details": str(e)}), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({"ok": False, "error": "db_write_failed", "details": str(e)}), 500

# ---------------- ASSIGN (manual) --------------------------------------
@app.route("/orders/<oid>/assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/assign", methods=["POST"])
def assign_driver(oid):
    body = request.json or {}
    driver_id = body.get("driver_id")
    if not driver_id:
        return jsonify({"ok": False, "error": "driver_id required"}), 400

    try:
        db = get_db()
        if not db.orders.find_one({"_internal_id": oid}):
            return jsonify({"ok": False, "error": "order not found"}), 404
        if not db.drivers.find_one({"_internal_id": driver_id, "active": True}):
            return jsonify({"ok": False, "error": "driver not found"}), 404

        db.orders.update_one({"_internal_id": oid}, {"$set": {
            "assigned_driver_id": driver_id, "assigned_at": _now_dt(), "status": "assigned"
        }})
        return jsonify({"ok": True}), 200

    except RuntimeError as e:
        return jsonify({"ok": False, "error": "db_unavailable", "details": str(e)}), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({"ok": False, "error": "db_write_failed", "details": str(e)}), 500

# ---------------- UPDATE STATUS (settlement on delivered) --------------
@app.route("/orders/<oid>/status", methods=["POST"])
@app.route("/api/app/orders/<oid>/status", methods=["POST"])
def update_status(oid):
    body = request.json or {}
    new_status = body.get("status")
    allowed = {"pending","assigned","in_transit","delivered","cancelled","failed","review_required"}
    if new_status not in allowed:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    try:
        db = get_db()
        o = db.orders.find_one({"_internal_id": oid})
        if not o: return jsonify({"ok": False, "error": "order not found"}), 404

        update_set = {"status": new_status}
        if new_status == "delivered":
            update_set["delivered_at"] = _now_dt()

            # compute settlement
            ck = o.get("cluster_key")
            since = _now_dt() - timedelta(minutes=CLUSTER_WINDOW_MIN)
            prior = db.orders.count_documents({
                "cluster_key": ck, "delivered_at": {"$gte": since}, "assigned_driver_id": o.get("assigned_driver_id")
            })
            driver_cut, platform_cut = compute_earnings(o, prior_in_cluster=max(0, prior-1))
            update_set["settlement"] = {"driver": driver_cut, "platform": platform_cut, "settled": False}

            # accrue to driver weekly balance
            if o.get("assigned_driver_id"):
                accrue_driver_earning(db, o["assigned_driver_id"], driver_cut, "delivery", o.get("order_id"))

        db.orders.update_one({"_internal_id": oid}, {"$set": update_set})
        return jsonify({"ok": True}), 200

    except RuntimeError as e:
        return jsonify({"ok": False, "error": "db_unavailable", "details": str(e)}), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({"ok": False, "error": "db_write_failed", "details": str(e)}), 500

# ---------------- DRIVERS ----------------------------------------------
@app.route("/drivers", methods=["POST"])
@app.route("/api/app/drivers", methods=["POST"])
def create_driver():
    data = request.json or {}
    internal_id = str(uuid.uuid4())

    doc = {
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
        "meta": data.get("meta", {})  # includes zone, radius_km
    }

    try:
        db = get_db()
        db.drivers.insert_one(doc)
        return jsonify({"ok": True, "driver_db_id": internal_id}), 201
    except RuntimeError as e:
        return jsonify({"ok": False, "error": "db_unavailable", "details": str(e)}), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({"ok": False, "error": "db_write_failed", "details": str(e)}), 500

@app.route("/drivers", methods=["GET"])
@app.route("/api/app/drivers", methods=["GET"])
def list_drivers():
    try:
        db = get_db()
        cur = db.drivers.find({"active": True})
        return jsonify({"ok": True, "drivers": [safe_doc(d) for d in cur]}), 200
    except RuntimeError as e:
        return jsonify({"ok": False, "error": "db_unavailable", "details": str(e), "drivers": []}), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({"ok": False, "error": "db_read_failed", "details": str(e), "drivers": []}), 500

# ---------------- PAYOUTS: weekly close --------------------------------
@app.route("/settlements/weekly-close", methods=["POST"])
@app.route("/api/app/settlements/weekly-close", methods=["POST"])
def weekly_close():
    """Create payout docs for all drivers and zero their weekly_payout_due."""
    body = request.json or {}
    note = body.get("note", "weekly close")
    try:
        db = get_db()
        cur = db.drivers.find({"active": True})
        created = []
        for d in cur:
            due = float(d.get("weekly_payout_due") or 0.0)
            if due <= 0: continue
            payout = {
                "driver_id": d["_internal_id"],
                "amount": round(due, 2),
                "note": note,
                "created_at": _now_dt(),
                "status": "pending"  # you can mark 'paid' after you transfer
            }
            db.payouts.insert_one(payout)
            db.drivers.update_one({"_internal_id": d["_internal_id"]}, {"$set": {"weekly_payout_due": 0.0}})
            created.append({"driver_id": d["_internal_id"], "amount": payout["amount"]})
        return jsonify({"ok": True, "payouts": created}), 200
    except RuntimeError as e:
        return jsonify({"ok": False, "error": "db_unavailable", "details": str(e)}), 500
    except mongo_errors.PyMongoError as e:
        return jsonify({"ok": False, "error": "db_write_failed", "details": str(e)}), 500
