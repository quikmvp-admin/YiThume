/* yithume-lite-pro.js — no-backend, localStorage-based ops for YiThume */
(function () {
  "use strict";
  // ====== CONFIG ======
  const CONFIG = {
    WHATSAPP_NUMBER: "270691456201",
    DRIVER_SHARE_FIRST: 1.00,
    DRIVER_SHARE_NEXT: 0.40,
    DEFAULT_MARKUP_PERCENT: 12,
    PayoutWeekDays: [1,2,3,4,5,6,0],
    ADMIN_BADGE_TEXT: "YiThume — Admin",
  };

  // ====== STORAGE KEYS ======
  const KEYS = {
    ORDERS: "yithume.orders.v1",
    DRIVERS: "yithume.drivers.v1",
    BATCHES: "yithume.batches.v1",
    PAYOUTS: "yithume.payouts.v1",
  };

  // ====== UTIL ======
  const pad2 = n => String(n).padStart(2, "0");
  const dayNames = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
  const nowISO = () => new Date().toISOString();
  const uuid = () => "YI-" + Date.now().toString().slice(-6);

  function load(key, fallback) {
    try { return JSON.parse(localStorage.getItem(key) || JSON.stringify(fallback)); }
    catch { return fallback; }
  }
  function save(key, value) { localStorage.setItem(key, JSON.stringify(value)); }

  function nextETA(rush) {
    const now = new Date();
    const windows = [[8,10],[12,14],[16,18]];
    const allowed = new Set([0,3,4,5,6]); // Sun, Wed–Sat
    const fmt = (date,[h1,h2]) =>
      date.toDateString() === new Date().toDateString()
      ? `${pad2(h1)}:00–${pad2(h2)}:00 ${rush ? "today (rush)":"today"}`
      : `${dayNames[date.getDay()]} ${pad2(h1)}:00–${pad2(h2)}:00`;
    if (allowed.has(now.getDay())) for (const w of windows) if (now.getHours() < w[1]) return fmt(now,w);
    for (let i=1;i<=7;i++){const d=new Date(now);d.setDate(now.getDate()+i); if(allowed.has(d.getDay())) return fmt(d,windows[0]);}
    return "TBC";
  }

  // Back-calc cost if not given: price = cost*(1+markup)
  function inferCostFromPrice(price) {
    const m = CONFIG.DEFAULT_MARKUP_PERCENT/100;
    return Math.round(price / (1+m));
  }

  // ====== PUBLIC CART READER (works with your page) ======
  function readCartFromPage() {
    const zoneSel = document.getElementById("zoneSelect");
    const rushToggle = document.getElementById("rushToggle");
    const fee = Number(zoneSel?.selectedOptions?.[0]?.dataset?.fee || 0);
    const rush = rushToggle?.checked ? 30 : 0;
    const zone = zoneSel?.value || "A";
    const pay = document.querySelector('input[name="pay"]:checked')?.value || "card";

    const checks = Array.from(document.querySelectorAll(".itemCheck"));
    const items = checks.filter(c => c.checked).map(c => {
      const idx = c.dataset.idx;
      const row = c.closest(".flex");
      const name = row?.querySelector("label span")?.textContent?.trim() || `Item ${idx}`;
      const qty = Number(document.querySelector(`.itemQty[data-idx="${idx}"]`)?.value || 1);
      const priceTxt = row?.querySelector(".text-slate-500")?.textContent || "R0";
      const price = Number((priceTxt.match(/\d+/)||[0])[0]);
      const cost = Number(row?.dataset?.cost || 0) || inferCostFromPrice(price);
      return { name, qty, price, cost };
    });

    const addrEl = document.getElementById("addrInput");
    const address = addrEl?.value?.trim() || "";
    return { items, zone, baseFee: fee, rushFee: rush, paymentChoice: pay, customer: { address } };
  }

  // ====== ORDER CREATION (WA message) ======
  function makeWhatsAppLink(payload) {
    const {
      whatsappNumber = CONFIG.WHATSAPP_NUMBER,
      items, zone, baseFee=0, rushFee=0, paymentChoice,
      etaLabel, customer={}
    } = payload;

    const orderId = uuid();
    const itemsText = items.map(i => `${i.name} x${i.qty}`).join(", ");
    const itemsTotal = items.reduce((s, i) => s + i.price*i.qty, 0);
    const total = itemsTotal + baseFee + rushFee;
    const eta = etaLabel || nextETA(Boolean(rushFee));
    const payLine = paymentChoice === "deposit_cod" ? "50% deposit + COD" : (paymentChoice?.toUpperCase() || "CARD");

    const msg =
      `Yi Thume, my order is:%0A- ${encodeURIComponent(itemsText)}%0A` +
      `Deliver to: ${encodeURIComponent(customer.address || "[paste address / pin]")}%0A` +
      `Zone: ${zone} (Delivery R${baseFee}${rushFee?` + Rush R${rushFee}`:""})%0A` +
      `Total: R${total}%0A` +
      `Expected delivery: ${encodeURIComponent(eta)}%0A` +
      `Payment choice: ${encodeURIComponent(payLine)}%0A` +
      `Ref: ${orderId}`;

    const link = `https://wa.me/${whatsappNumber}?text=${msg}`;

    // Persist order
    const orders = load(KEYS.ORDERS, []);
    orders.push({
      orderId, zone, items, itemsText, baseFee, rushFee, total,
      payment_choice: paymentChoice,
      eta_label: eta,
      customer,
      status: "awaiting_payment",
      created_at: nowISO()
    });
    save(KEYS.ORDERS, orders);

    return { orderId, whatsapp_link: link, total, eta };
  }

  // ====== DRIVER, ASSIGNMENT, EARNINGS ======
  function addDriver({name, phone, zone, radius_km=10, vehicle="Bike"}) {
    const drivers = load(KEYS.DRIVERS, []);
    const d = { id: "DRV-"+Date.now().toString().slice(-6), name, phone, zone, radius_km, vehicle, is_active: true, created_at: nowISO() };
    drivers.push(d);
    save(KEYS.DRIVERS, drivers);
    return d;
  }

  function clusterKey(order) {
    const a = (order.customer?.address || "").toLowerCase().replace(/\s+/g," ").trim();
    if (a) return `${order.zone}::${a}`;
    return `${order.zone}::zone-only`;
  }

  function pickDriverForZone(zone) {
    const drivers = load(KEYS.DRIVERS, []);
    return drivers.find(d => d.zone === zone && d.is_active) || drivers[0] || null;
  }

  function computeClusterEarnings(n, feeEach) {
    const first = feeEach * CONFIG.DRIVER_SHARE_FIRST;
    const rest = Math.max(0, n-1) * feeEach * CONFIG.DRIVER_SHARE_NEXT;
    const driverTotal = Math.round(first + rest);
    const allFees = n * feeEach;
    const platformDeliveryMargin = Math.round(allFees - driverTotal);
    return { driverTotal, platformDeliveryMargin };
  }

  function markOrderPaid(orderId) {
    const orders = load(KEYS.ORDERS, []);
    const idx = orders.findIndex(o => o.orderId === orderId);
    if (idx >= 0) { orders[idx].status = "paid"; orders[idx].paid_at = nowISO(); save(KEYS.ORDERS, orders); return true; }
    return false;
  }

  function autoAssign() {
    const orders = load(KEYS.ORDERS, []);
    const batches = load(KEYS.BATCHES, []);
    const byKey = {};
    const feeOf = o => Number(o.baseFee||0) + Number(o.rushFee||0);

    const candidates = orders.filter(o => o.status === "paid" && !o.assigned_batch_id);

    for (const o of candidates) {
      const k = clusterKey(o);
      if (!byKey[k]) byKey[k] = [];
      byKey[k].push(o);
    }

    const created = [];

    for (const k of Object.keys(byKey)) {
      const group = byKey[k];
      if (!group.length) continue;
      const zone = group[0].zone;
      const driver = pickDriverForZone(zone);
      if (!driver) continue;

      const feeEach = feeOf(group[0]);
      const { driverTotal, platformDeliveryMargin } = computeClusterEarnings(group.length, feeEach);

      const batch = {
        id: "BAT-"+Date.now().toString().slice(-6)+"-"+Math.floor(Math.random()*100),
        zone,
        cluster_key: k,
        driver,
        orders: group.map(o => o.orderId),
        delivery_fee_each: feeEach,
        driver_earnings: driverTotal,
        platform_delivery_margin: platformDeliveryMargin,
        status: "assigned",
        started_at: nowISO()
      };

      for (const o of group) {
        const idx = orders.findIndex(x => x.orderId === o.orderId);
        if (idx >= 0) {
          orders[idx].status = "assigned";
          orders[idx].assigned_batch_id = batch.id;
        }
      }

      batches.push(batch);
      created.push(batch);
    }

    save(KEYS.ORDERS, orders);
    save(KEYS.BATCHES, batches);
    return created;
  }

  function completeBatch(batchId) {
    const orders = load(KEYS.ORDERS, []);
    const batches = load(KEYS.BATCHES, []);
    const bIdx = batches.findIndex(b => b.id === batchId);
    if (bIdx < 0) return false;

    for (const oid of (batches[bIdx].orders||[])) {
      const oIdx = orders.findIndex(x => x.orderId === oid);
      if (oIdx >= 0) orders[oIdx].status = "delivered";
    }
    batches[bIdx].status = "completed";
    batches[bIdx].completed_at = nowISO();

    save(KEYS.ORDERS, orders);
    save(KEYS.BATCHES, batches);
    return true;
  }

  // ====== WEEKLY PAYOUTS ======
  function startOfMonday(d = new Date()) {
    const date = new Date(d);
    const day = date.getDay();
    const diff = (day + 6) % 7;
    date.setHours(0,0,0,0);
    date.setDate(date.getDate() - diff);
    return date;
  }
  function endOfSunday(fromMonday) {
    const d = new Date(fromMonday);
    d.setDate(d.getDate()+6);
    d.setHours(23,59,59,999);
    return d;
  }
  function weekLabel(d=new Date()) {
    const onejan = new Date(d.getFullYear(),0,1);
    const week = Math.ceil((((d - onejan) / 86400000) + onejan.getDay()+1)/7);
    return `${d.getFullYear()}-W${String(week).padStart(2,"0")}`;
  }

  function generateWeeklyPayouts(forDate = new Date()) {
    const monday = startOfMonday(forDate);
    const sunday = endOfSunday(monday);
    const batches = load(KEYS.BATCHES, []);
    const inRange = batches.filter(b => b.status === "completed" && new Date(b.completed_at) >= monday && new Date(b.completed_at) <= sunday);

    const grouped = {};
    for (const b of inRange) {
      const id = b.driver.id;
      if (!grouped[id]) grouped[id] = { driver: b.driver, earnings: 0, batches: 0, orders: 0 };
      grouped[id].earnings += (b.driver_earnings || 0);
      grouped[id].batches += 1;
      grouped[id].orders += (b.orders?.length || 0);
    }

    const payouts = load(KEYS.PAYOUTS, []);
    const week = weekLabel(monday);
    const out = [];

    for (const id of Object.keys(grouped)) {
      const g = grouped[id];
      const p = {
        id: "PO-"+Date.now().toString().slice(-6)+"-"+Math.floor(Math.random()*100),
        week_label: week,
        from: monday.toISOString(),
        to: sunday.toISOString(),
        driver: g.driver,
        earnings: Math.round(g.earnings),
        orders_count: g.orders,
        batches_count: g.batches,
        status: "pending",
        created_at: nowISO(),
        ref: `YI-${week}-${id.slice(-4)}`
      };
      payouts.push(p);
      out.push(p);
    }

    save(KEYS.PAYOUTS, payouts);
    return out;
  }

  function markPayoutPaid(payoutId) {
    const payouts = load(KEYS.PAYOUTS, []);
    const idx = payouts.findIndex(p => p.id === payoutId);
    if (idx >= 0) {
      payouts[idx].status = "paid";
      payouts[idx].paid_at = nowISO();
      save(KEYS.PAYOUTS, payouts);
      return true;
    }
    return false;
  }

  // ====== ADMIN MINI-PANEL ======
  function injectAdminPanel() {
    const wrap = document.createElement("div");
    wrap.style.cssText = "position:fixed;right:12px;bottom:12px;z-index:99999;background:#0f172a;color:#fff;padding:10px 12px;border-radius:14px;box-shadow:0 6px 24px rgba(0,0,0,.25);font:12px/1.2 system-ui;max-width:320px";
    wrap.innerHTML = `
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
        <span style="display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;background:#10b981;border-radius:6px">✓</span>
        <b>${CONFIG.ADMIN_BADGE_TEXT}</b>
      </div>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        <button data-act="mark-paid"  style="padding:6px 8px;border-radius:10px;border:1px solid #334155;background:#111827;color:#fff">Mark Paid (Ref)</button>
        <button data-act="assign"     style="padding:6px 8px;border-radius:10px;border:1px solid #334155;background:#111827;color:#fff">Auto-Assign</button>
        <button data-act="complete"   style="padding:6px 8px;border-radius:10px;border:1px solid #334155;background:#111827;color:#fff">Complete Batch</button>
        <button data-act="payouts"    style="padding:6px 8px;border-radius:10px;border:1px solid #334155;background:#111827;color:#fff">Generate Weekly</button>
        <button data-act="drivers"    style="padding:6px 8px;border-radius:10px;border:1px solid #334155;background:#111827;color:#fff">Add Driver</button>
      </div>
      <div id="yt-admin-log" style="margin-top:8px;max-height:160px;overflow:auto;background:#0b1220;border-radius:10px;padding:6px"></div>
    `;
    document.body.appendChild(wrap);

    const log = (html) => {
      const box = wrap.querySelector("#yt-admin-log");
      const line = document.createElement("div");
      line.style.margin = "6px 0";
      line.innerHTML = html;
      box.appendChild(line);
      box.scrollTop = box.scrollHeight;
    };

    wrap.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-act]");
      if (!btn) return;
      const act = btn.dataset.act;

      if (act === "mark-paid") {
        const ref = prompt("Enter Order Ref (e.g., YI-123456)");
        if (!ref) return;
        const ok = markOrderPaid(ref);
        log(ok ? `✅ Marked paid: <b>${ref}</b>` : `⚠️ Not found: <b>${ref}</b>`);

      } else if (act === "assign") {
        const batches = autoAssign();
        if (!batches.length) return log("ℹ️ Nothing to assign");
        for (const b of batches) {
          log(`📦 Assigned <b>${b.orders.length}</b> to <b>${b.driver.name}</b> • Batch <b>${b.id}</b> • Driver earns <b>R${b.driver_earnings}</b>`);
        }

      } else if (act === "complete") {
        const id = prompt("Enter Batch ID (e.g., BAT-123456-42)");
        if (!id) return;
        const ok = completeBatch(id);
        log(ok ? `✅ Completed batch: <b>${id}</b>` : `⚠️ Batch not found`);

      } else if (act === "payouts") {
        const out = generateWeeklyPayouts();
        if (!out.length) return log("ℹ️ No completed batches this week");
        for (const p of out) {
          log(`💸 Payout <b>${p.week_label}</b> • <b>${p.driver.name}</b> gets <b>R${p.earnings}</b> • Ref <b>${p.ref}</b>`);
        }

      } else if (act === "drivers") {
        const name = prompt("Driver name"); if (!name) return;
        const phone = prompt("Driver phone (WhatsApp)"); if (!phone) return;
        const zone = prompt("Driver zone (A/B/C/D/E)","A") || "A";
        const d = addDriver({name, phone, zone});
        log(`👤 Added driver <b>${d.name}</b> (${d.phone}) in zone <b>${d.zone}</b>`);
      }
    });
  }

  // ====== HOOK DRIVER SIGN-UP ======
  function hookSignupForm() {
    const form = document.getElementById("driverFormSignup");
    if (!form) return;
    form.addEventListener("submit", () => {
      try {
        const name = document.getElementById("drvName").value;
        const phone = document.getElementById("drvPhone").value;
        const zone = document.getElementById("drvZone").value;
        const radius = Number(document.getElementById("drvRadius").value || 10);
        const vehicle = document.getElementById("drvVehicle").value || "Bike";
        addDriver({ name, phone, zone, radius_km: radius, vehicle });
      } catch (_) {}
    }, { once: true });
  }

  // ====== PUBLIC API ======
  window.YiThumeLitePro = {
    readCartFromPage,
    makeWhatsAppLink,
    markOrderPaid, autoAssign, completeBatch,
    generateWeeklyPayouts, markPayoutPaid
  };

  // Boot
  window.addEventListener("DOMContentLoaded", () => {
    injectAdminPanel();
    hookSignupForm();
  });
})();
