import os
import re
import uuid
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt

from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING, DESCENDING, errors as mongo_errors

# -------------------------------------------------
# ENV + MONGO
# -------------------------------------------------
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://username:password@cluster0.mongodb.net/yithume?retryWrites=true&w=majority"
)
mongo_client = MongoClient(MONGO_URI)
DB_NAME = "yithume"

def get_db():
    """Return a live db handle if Mongo works, else raise RuntimeError."""
    try:
        mongo_client.admin.command("ping")
        return mongo_client[DB_NAME]
    except Exception as e:
        raise RuntimeError(f"Mongo connection failed: {e}")

# -------------------------------------------------
# CONFIG / CONSTANTS
# -------------------------------------------------
ITEM_MARGIN_RATE = 0.12      # 12% margin if no explicit item.cost
PLATFORM_FEE_RATE = 0.10     # 10% of delivery fee goes to platform on single-drop
BATCH_BONUS_PER_EXTRA = 0.25 # each extra stop adds 25% of fee
BATCH_BONUS_CAP = 0.60       # cap bonus at +60% of fee total
CLUSTER_WINDOW_MIN = 120     # 2hr batch window
AUTO_ASSIGN_RADIUS_KM = 12   # max km to auto-assign driver
SERVICE_BBOX = {             # rough EC bounding box (tune later)
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
def _now_dt():
    return datetime.utcnow()

def _now_iso():
    return _now_dt().isoformat() + "Z"

def make_order_public_id():
    ts = datetime.utcnow().strftime("%Y%m%d")
    return f"YI-{ts}-{str(uuid.uuid4())[:6].upper()}"

def safe_doc(doc):
    """Strip Mongo _id and turn datetimes into ISO strings for JSON."""
    if not doc:
        return None
    out = dict(doc)
    out.pop("_id", None)

    for k in ("created_at", "assigned_at", "delivered_at"):
        if isinstance(out.get(k), datetime):
            out[k] = out[k].isoformat() + "Z"

    loc = out.get("current_location")
    if loc and isinstance(loc.get("updated_at"), datetime):
        loc["updated_at"] = loc["updated_at"].isoformat() + "Z"

    return out

def phone_ok(p):
    # WhatsApp numbers like "2782..." etc (10-15 digits)
    return bool(re.fullmatch(r"\d{10,15}", str(p or "").strip()))

def inside_service_area(lat, lng):
    # If we don't have coords, allow it (don't hard-block). We'll tighten later.
    if lat is None or lng is None:
        return True
    bb = SERVICE_BBOX
    return (
        bb["min_lat"] <= lat <= bb["max_lat"] and
        bb["min_lng"] <= lng <= bb["max_lng"]
    )

def haversine_km(lat1, lon1, lat2, lon2):
    """Distance between 2 lat/lng points in KM. Returns None if missing."""
    if None in (lat1, lon1, lat2, lon2):
        return None
    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2 * r * asin(sqrt(a))

def ensure_indexes(db):
    """Create useful indexes if they don't exist (idempotent)."""
    db.orders.create_index([("created_at", DESCENDING)])
    db.orders.create_index([("_internal_id", ASCENDING)], unique=True)
    db.orders.create_index([("customer.phone", ASCENDING), ("created_at", DESCENDING)])
    db.orders.create_index([("status", ASCENDING), ("created_at", DESCENDING)])
    db.orders.create_index([("cluster_key", ASCENDING)])
    db.orders.create_index([("assigned_driver_id", ASCENDING), ("delivered_at", DESCENDING)])

    db.drivers.create_index([("_internal_id", ASCENDING)], unique=True)
    db.drivers.create_index([("active", ASCENDING), ("available", ASCENDING), ("meta.zone", ASCENDING)])
    db.drivers.create_index([("current_location.lat", ASCENDING), ("current_location.lng", ASCENDING)])

    db.zone_demand.create_index([("zone", ASCENDING), ("ts", DESCENDING)])

    db.payouts.create_index([("driver_id", ASCENDING), ("created_at", DESCENDING)])

def wa_order_text(order):
    """Build WhatsApp message body sent back to frontend."""
    items_list = ", ".join(
        [f"{i.get('name')} x{i.get('qty')}" for i in order.get("items", [])]
    )
    addr = order.get("customer", {}).get("address", {})
    eta  = order.get("route", {}).get("eta_text", "TBC")
    total = order.get("total", 0)
    zone  = order.get("meta", {}).get("zone", "")
    pay_m = order.get("payment", {}).get("method", "card")

    lines = [
        "Yi Thume, my order is:",
        f"Ref: {order.get('order_id')}",
        f"Items: {items_list}",
        f"Deliver to: {addr.get('line1','')} (Zone {zone})",
        f"Total: R{total}",
        f"Expected delivery: {eta}",
        f"Payment: {pay_m}",
        "Please confirm + send payment link."
    ]
    return "\n".join(lines)

# ---------------- Fraud / legitimacy rules -----------------------------
def rule_based_fraud_score(db, order_doc):
    """
    Very lightweight heuristics:
    - bad phone
    - same phone blasting multiple orders in last 60m
    - duplicate subtotal in last 10m
    - clearly outside service bbox
    - way above rolling avg spend
    """
    score = 0.0
    flags = {}

    phone = (order_doc.get("customer") or {}).get("phone")
    if not phone_ok(phone):
        flags["bad_phone"] = True
        score += 0.2

    # phone velocity (orders in last 60 minutes)
    recent_count = (
        db.orders.count_documents({
            "customer.phone": phone,
            "created_at": {"$gte": _now_dt() - timedelta(minutes=60)}
        }) if phone else 0
    )
    if recent_count >= 3:
        flags["phone_velocity"] = True
        score += 0.4

    # duplicate-ish subtotal in last 10m
    if phone:
        dup = db.orders.find_one({
            "customer.phone": phone,
            "subtotal": order_doc.get("subtotal", 0),
            "created_at": {"$gte": _now_dt() - timedelta(minutes=10)}
        })
        if dup:
            flags["duplicate_like"] = True
            score += 0.3

    # rough out-of-area check
    coords = (((order_doc.get("customer") or {}).get("address") or {}).get("coords") or {})
    if not inside_service_area(coords.get("lat"), coords.get("lng")):
        flags["out_of_area"] = True
        score += 0.5

    # extremely high basket vs avg
    pipeline = [{"$group": {"_id": None, "avg": {"$avg": "$total"}}}]
    agg = list(db.orders.aggregate(pipeline))
    avg_total = agg[0]["avg"] if agg else 50  # default baseline ~R50
    if order_doc.get("total", 0) > avg_total * 3:
        flags["high_value"] = True
        score += 0.2

    return min(score, 1.0), flags

# ---------------- Driver availability / dispatch -----------------------
def find_available_driver(db, zone, drop_lat=None, drop_lng=None):
    """
    1. Prefer drivers in same zone, active+available.
    2. If we have customer coords, choose closest within AUTO_ASSIGN_RADIUS_KM.
    3. Otherwise just first matching driver.
    """
    q = {"active": True, "available": True}
    if zone:
        q["meta.zone"] = zone

    candidates = list(db.drivers.find(q))
    if not candidates:
        return None

    if drop_lat is None or drop_lng is None:
        return candidates[0]

    best = None
    best_d = 1e9
    for d in candidates:
        loc = (d.get("current_location") or {})
        km = haversine_km(drop_lat, drop_lng, loc.get("lat"), loc.get("lng"))
        if km is None:
            continue
        if km <= AUTO_ASSIGN_RADIUS_KM and km < best_d:
            best = d
            best_d = km

    return best or candidates[0]

def cluster_key(order_doc):
    """
    Used to group drops for batching / driver earnings.
    We cluster by (zone + coarse address keyword + 2hr window start).
    """
    addr  = ((order_doc.get("customer") or {}).get("address") or {})
    zone  = (order_doc.get("meta") or {}).get("zone", "")
    line1 = (addr.get("line1") or "").strip().lower()

    # Grab first word / landmark as coarse anchor
    coarse = re.split(r"[,\s]+", line1)[0] if line1 else "unknown"

    now = _now_dt()
    # round hour down to nearest 2h block, e.g. 13:xx -> 12:00 block
    block_hours = (now.hour // (CLUSTER_WINDOW_MIN // 60)) * (CLUSTER_WINDOW_MIN // 60)
    window_start = now.replace(hour=block_hours, minute=0, second=0, microsecond=0)
    bucket_str = window_start.strftime("%Y%m%d%H%M")

    return f"{zone}:{coarse}:{bucket_str}"

# ---------------- Earnings / settlement logic --------------------------
def compute_earnings(order_doc, prior_in_cluster=0):
    """
    We split delivery_fee between driver and platform, and we apply batching
    bonus if same driver is doing multiple clustered drops in same 2h block.
    We also add margin on items for the platform.
    """
    fee = float(order_doc.get("delivery_fee", 0))

    # baseline split
    platform_cut = fee * PLATFORM_FEE_RATE
    driver_cut   = fee - platform_cut

    # batching bonus: each extra drop in that cluster gives driver more money
    # prior_in_cluster = number of previous delivered orders in this cluster window
    if prior_in_cluster > 0:
        # e.g. 1 extra stop => +25% of fee to driver
        bonus_pct = min(prior_in_cluster * BATCH_BONUS_PER_EXTRA, BATCH_BONUS_CAP)
        bonus_amt = bonus_pct * fee
        driver_cut += bonus_amt
        platform_cut = max(0.0, fee - driver_cut)

    # product margin
    items = order_doc.get("items", [])
    margin = 0.0
    for it in items:
        price = float(it.get("price", 0))
        qty   = int(it.get("qty", 1))
        cost  = it.get("cost")
        if cost is not None:
            margin += max(0.0, price - float(cost)) * qty
        else:
            margin += (price * ITEM_MARGIN_RATE) * qty

    platform_total = platform_cut + margin

    return round(driver_cut, 2), round(platform_total, 2)

def accrue_driver_earning(db, driver_internal_id, amount, reason, order_id):
    """
    Add money to driver's weekly_payout_due and append to earnings_history.
    """
    db.drivers.update_one(
        {"_internal_id": driver_internal_id},
        {
            "$inc": {"weekly_payout_due": amount},
            "$push": {"earnings_history": {
                "amount": amount,
                "reason": reason,
                "order_id": order_id,
                "ts": _now_dt()
            }}
        }
    )

# ---------------- Demand heatlog (no-driver zones) ---------------------
def log_zone_demand(db, zone, coords, phone):
    """Record when someone tried to order in a zone but no driver was available."""
    db.zone_demand.insert_one({
        "zone": zone,
        "ts": _now_dt(),
        "phone": phone,
        "coords": coords
    })

def recent_zone_demand_snapshot(db):
    """
    For admin dashboard heat bubbles.
    Count how many 'no driver available' events per zone in the last 24h.
    Returns dict like { "A":{"misses":2}, "C":{"misses":1} }
    """
    since = _now_dt() - timedelta(hours=24)
    pipe = [
        {"$match": {"ts": {"$gte": since}}},
        {"$group": {"_id": "$zone", "count": {"$sum": 1}}},
    ]
    out = {}
    for row in db.zone_demand.aggregate(pipe):
        z = row["_id"] or "?"
        out[z] = {"misses": row["count"]}
    return out

# -------------------------------------------------
# STARTUP: try indexes
# -------------------------------------------------
try:
    ensure_indexes(get_db())
except Exception:
    # On cold boot without working DB, just continue.
    pass

# -------------------------------------------------
# ROUTES
# -------------------------------------------------

@app.route("/", methods=["GET"])
@app.route("/api/app", methods=["GET"])
def health():
    """
    Health check for uptime monitors and debugging.
    Frontend doesn't call this for user flow, but it's nice to test:
    GET /api/app
    """
    try:
        db = get_db()
        orders_count = db.orders.count_documents({})
        drivers_count = db.drivers.count_documents({"active": True})
        return jsonify({
            "ok": True,
            "service": "YiThume (mongo+logic)",
            "db": "up",
            "orders_count": orders_count,
            "drivers_count": drivers_count
        }), 200
    except RuntimeError as e:
        return jsonify({
            "ok": True,
            "service": "YiThume (mongo+logic)",
            "db": "down",
            "error": str(e)
        }), 200

# ---------------- CREATE ORDER (front-end Checkout button) -------------
@app.route("/orders", methods=["POST"])
@app.route("/api/app/orders", methods=["POST"])
def create_order():
    """
    Frontend calls this when user hits Checkout in the modal.
    We:
    - verify driver exists in that zone / area
    - run fraud heuristics
    - save order
    - return wa_message so frontend can open WhatsApp with it
    """
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

        "payment": data.get("payment", {
            "method": "cash",
            "status": "pending",
            "provider_ref": None
        }),

        "status": "pending",            # may become 'review_required'
        "assigned_driver_id": None,
        "assigned_at": None,
        "delivered_at": None,

        "route": data.get("route", {}), # contains eta_text from frontend
        "created_by": data.get("created_by", "web"),
        "meta": data.get("meta", {}),   # contains zone, rush, etc

        "fraud_score": 0.0,
        "fraud_flags": {},
        "cluster_key": None,

        "settlement": {
            "driver": 0.0,
            "platform": 0.0,
            "settled": False
        }
    }

    try:
        db = get_db()

        # STEP 1: gate based on driver availability for that zone / area
        zone   = (order_doc["meta"] or {}).get("zone")
        coords = (((order_doc.get("customer") or {}).get("address") or {}).get("coords") or {})
        candidate_driver = find_available_driver(
            db,
            zone,
            coords.get("lat"),
            coords.get("lng")
        )

        if not candidate_driver:
            # log demand so admin can see "Zone C = high demand, no drivers"
            log_zone_demand(
                db,
                zone,
                coords,
                (order_doc.get("customer") or {}).get("phone")
            )

            return jsonify({
                "ok": False,
                "error": "no_driver_available",
                "zone": zone,
                "message": (
                    f"Sorry, no driver is currently available in Zone {zone}. "
                    f"Please try again later."
                )
            }), 409

        # STEP 2: fraud / trust
        fs, ff = rule_based_fraud_score(db, order_doc)
        order_doc["fraud_score"], order_doc["fraud_flags"] = fs, ff
        if fs >= 0.75:
            order_doc["status"] = "review_required"

        # STEP 3: cluster key for batching economics
        order_doc["cluster_key"] = cluster_key(order_doc)

        # STEP 4: save
        db.orders.insert_one(order_doc)

        # STEP 5: build WhatsApp message for frontend
        wa_msg = wa_order_text(order_doc)

        # STEP 6: also send fresh zone_demand snapshot so admin panel can show heat
        zd_snapshot = recent_zone_demand_snapshot(db)

        return jsonify({
            "ok": True,
            "order_db_id": internal_id,
            "order_public_id": public_id,
            "status": order_doc["status"],
            "fraud_score": fs,
            "fraud_flags": ff,
            "wa_message": wa_msg,
            "zone_demand_snapshot": zd_snapshot
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

# ---------------- LIST ORDERS (admin panel tabs) -----------------------
@app.route("/orders", methods=["GET"])
@app.route("/api/app/orders", methods=["GET"])
def list_orders():
    """
    Admin panel calls this with ?status=pending / assigned / delivered / etc
    We also return zone_demand_snapshot so you can see where you need drivers.
    """
    status = request.args.get("status")
    q = {"status": status} if status else {}

    try:
        db = get_db()
        cur = db.orders.find(q).sort("created_at", DESCENDING).limit(50)
        orders_out = [safe_doc(o) for o in cur]

        zd_snapshot = recent_zone_demand_snapshot(db)

        return jsonify({
            "ok": True,
            "orders": orders_out,
            "zone_demand_snapshot": zd_snapshot
        }), 200

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

# ---------------- AUTO-ASSIGN (best nearby driver) ---------------------
@app.route("/orders/<oid>/auto-assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/auto-assign", methods=["POST"])
def auto_assign(oid):
    """
    Future: from admin "Auto-Assign" button.
    Finds best driver and marks order assigned.
    """
    try:
        db = get_db()
        o = db.orders.find_one({"_internal_id": oid})
        if not o:
            return jsonify({"ok": False, "error": "order not found"}), 404

        zone = (o.get("meta") or {}).get("zone")
        coords = (((o.get("customer") or {}).get("address") or {}).get("coords") or {})
        d = find_available_driver(
            db,
            zone,
            coords.get("lat"),
            coords.get("lng")
        )

        if not d:
            return jsonify({"ok": False, "error": "no_driver_available"}), 409

        db.orders.update_one(
            {"_internal_id": oid},
            {"$set": {
                "assigned_driver_id": d["_internal_id"],
                "assigned_at": _now_dt(),
                "status": "assigned"
            }}
        )
        return jsonify({
            "ok": True,
            "driver_id": d["_internal_id"]
        }), 200

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

# ---------------- MANUAL ASSIGN (admin picks driver) -------------------
@app.route("/orders/<oid>/assign", methods=["POST"])
@app.route("/api/app/orders/<oid>/assign", methods=["POST"])
def assign_driver(oid):
    """
    Admin manually chooses driver_id and posts {driver_id:"..."}
    """
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

        db.orders.update_one(
            {"_internal_id": oid},
            {"$set": {
                "assigned_driver_id": driver_id,
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

# ---------------- UPDATE STATUS (driver flow / delivery complete) ------
@app.route("/orders/<oid>/status", methods=["POST"])
@app.route("/api/app/orders/<oid>/status", methods=["POST"])
def update_status(oid):
    """
    Driver (or admin) moves order through lifecycle.
    When we hit 'delivered':
    - timestamp delivered_at
    - compute settlement for that driver
    - accrue payout to driver's weekly_payout_due
    """
    body = request.json or {}
    new_status = body.get("status")

    allowed = {
        "pending", "assigned", "in_transit",
        "delivered", "cancelled", "failed",
        "review_required"
    }
    if new_status not in allowed:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    try:
        db = get_db()
        o = db.orders.find_one({"_internal_id": oid})
        if not o:
            return jsonify({"ok": False, "error": "order not found"}), 404

        update_set = {"status": new_status}

        if new_status == "delivered":
            update_set["delivered_at"] = _now_dt()

            # batch economics:
            ck = o.get("cluster_key")
            since = _now_dt() - timedelta(minutes=CLUSTER_WINDOW_MIN)
            # how many orders in same bucket already delivered recently by same driver
            prior = db.orders.count_documents({
                "cluster_key": ck,
                "delivered_at": {"$gte": since},
                "assigned_driver_id": o.get("assigned_driver_id")
            })

            driver_cut, platform_cut = compute_earnings(
                o,
                prior_in_cluster=max(0, prior - 1)
            )

            update_set["settlement"] = {
                "driver": driver_cut,
                "platform": platform_cut,
                "settled": False
            }

            # push money onto driver weekly ledger
            if o.get("assigned_driver_id"):
                accrue_driver_earning(
                    db,
                    o["assigned_driver_id"],
                    driver_cut,
                    "delivery",
                    o.get("order_id")
                )

        db.orders.update_one(
            {"_internal_id": oid},
            {"$set": update_set}
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

# ---------------- DRIVERS ----------------------------------------------
@app.route("/drivers", methods=["POST"])
@app.route("/api/app/drivers", methods=["POST"])
def create_driver():
    """
    Called when a driver signs up in the modal.
    We create a driver record and mark them available in their zone.
    """
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

        "ratings": {
            "count": 0,
            "avg": None
        },

        "meta": data.get("meta", {})  # e.g. { zone:"C", radius_km:"10" }
    }

    try:
        db = get_db()
        db.drivers.insert_one(doc)
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

@app.route("/drivers", methods=["GET"])
@app.route("/api/app/drivers", methods=["GET"])
def list_drivers():
    """
    Admin / debug endpoint to show active drivers.
    """
    try:
        db = get_db()
        cur = db.drivers.find({"active": True})
        return jsonify({
            "ok": True,
            "drivers": [safe_doc(d) for d in cur]
        }), 200

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

# ---------------- PAYOUTS WEEKLY CLOSE ---------------------------------
@app.route("/settlements/weekly-close", methods=["POST"])
@app.route("/api/app/settlements/weekly-close", methods=["POST"])
def weekly_close():
    """
    Manual admin action:
    - snapshot each driver's weekly_payout_due into payouts[]
    - reset their weekly_payout_due to 0
    (you still actually send them money offline, this just records it)
    """
    body = request.json or {}
    note = body.get("note", "weekly close")

    try:
        db = get_db()
        cur = db.drivers.find({"active": True})

        created = []
        for d in cur:
            due = float(d.get("weekly_payout_due") or 0.0)
            if due <= 0:
                continue

            payout = {
                "driver_id": d["_internal_id"],
                "amount": round(due, 2),
                "note": note,
                "created_at": _now_dt(),
                "status": "pending"  # later you can mark 'paid'
            }
            db.payouts.insert_one(payout)

            # reset driver's running balance
            db.drivers.update_one(
                {"_internal_id": d["_internal_id"]},
                {"$set": {"weekly_payout_due": 0.0}}
            )

            created.append({
                "driver_id": d["_internal_id"],
                "amount": payout["amount"]
            })

        return jsonify({
            "ok": True,
            "payouts": created
        }), 200

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

# no app.run(); Vercel will import `app`
