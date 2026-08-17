#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# وكيل التنفيذ الذكي — ينفذ توصيات وكيل استراتيجية الاستثمار
import json
import os
import re
import ssl
import threading
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"
DATA_DIR.mkdir(exist_ok=True)
PUBLIC = BASE / "public"
PORT_FILE = DATA_DIR / "port.txt"
SETTINGS_FILE = DATA_DIR / "settings.json"
PORTFOLIO_FILE = DATA_DIR / "portfolio.json"
ORDERS_FILE = DATA_DIR / "orders.json"
SESSION_FILE = DATA_DIR / "session.json"
REPORTS_FILE = DATA_DIR / "reports.json"
STRATEGY_FILE = DATA_DIR / "strategy_cache.json"
HISTORY_FILE = DATA_DIR / "history.json"

DEFAULT_PORT = 8086
DEFAULT_STRATEGY_URL = os.environ.get("STRATEGY_URL", "http://127.0.0.1:8085")
MONITOR_INTERVAL = int(os.environ.get("MONITOR_MINUTES", "15"))   # دقائق: متابعة
KEEPALIVE_INTERVAL = int(os.environ.get("KEEPALIVE_MINUTES", "10"))  # دقائق: إبقاء الجلسة
MAX_REPORTS = 60

_session_lock = threading.RLock()
_portfolio_lock = threading.RLock()
_orders_lock = threading.RLock()
_strategy_lock = threading.RLock()
_loop_lock = threading.Lock()

_state = {
    "strategy": None,          # آخر استراتيجية
    "strategy_fetched_at": None,
    "strategy_error": None,
    "last_decision": None,
    "last_report": None,
    "executing": False,
    "executor_error": None,
}


# ---------------- أدوات مساعدة ----------------
def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_ts(v):
    try:
        d = datetime.fromisoformat(v.replace("Z", "+00:00"))
        d = d.astimezone()
        return d.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return v or "-"


def money(v):
    try:
        return round(float(v))
    except Exception:
        return 0


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def get_settings():
    s = load_json(SETTINGS_FILE, {})
    s.setdefault("auto_execute", False)
    s.setdefault("executor_webhook", "")
    s.setdefault("report_webhook", "")
    s.setdefault("confirm_orders", True)
    return s


def save_settings(s):
    save_json(SETTINGS_FILE, s)


def get_session():
    return load_json(SESSION_FILE, {"state": "off"})


def save_session(s):
    save_json(SESSION_FILE, s)


def get_portfolio():
    return load_json(PORTFOLIO_FILE, [])


def save_portfolio(p):
    save_json(PORTFOLIO_FILE, p)


def get_orders():
    return load_json(ORDERS_FILE, [])


def save_orders(o):
    save_json(ORDERS_FILE, o)


def get_history():
    return load_json(HISTORY_FILE, [])


def save_history(h):
    save_json(HISTORY_FILE, h)


def get_reports():
    return load_json(REPORTS_FILE, [])


def save_reports(r):
    save_json(REPORTS_FILE, r)


def http_get(url, timeout=15, data=None, headers=None):
    hd = {"User-Agent": "executive-agent/1.0"}
    if headers:
        hd.update(headers)
    req = urllib.request.Request(url, data=data, headers=hd, method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as ex:
        return 0, str(ex)


def http_json(url, timeout=15):
    code, body = http_get(url, timeout=timeout)
    if code != 200:
        raise RuntimeError(f"HTTP {code}: {url} ({body[:120]})")
    try:
        return json.loads(body)
    except Exception as ex:
        raise RuntimeError(f"JSON غير صالح من {url}: {ex}")


# ---------------- جلب الاستراتيجية ----------------
def fetch_strategy():
    try:
        payload = http_json(DEFAULT_STRATEGY_URL + "/api/strategy", timeout=20)
        if isinstance(payload, dict) and payload.get("ok") is False:
            raise RuntimeError(str(payload.get("error", "خطأ في وكيل الاستراتيجية")))
        s = payload.get("strategy") or payload
        with _strategy_lock:
            _state["strategy"] = s
            _state["strategy_fetched_at"] = now_iso()
            _state["strategy_error"] = None
            save_json(STRATEGY_FILE, s)
        return s
    except Exception as ex:
        with _strategy_lock:
            _state["strategy_error"] = str(ex)
            cached = load_json(STRATEGY_FILE, None)
            if cached and _state["strategy"] is None:
                _state["strategy"] = cached
        return _state["strategy"]


def strategy_connected():
    try:
        r = http_json(DEFAULT_STRATEGY_URL + "/api/status", timeout=10)
        return bool(r.get("ok"))
    except Exception:
        return False


# ---------------- محفظة ----------------
def portfolio_lookup(portfolio, company):
    for p in portfolio:
        if p.get("company") == company:
            return p
    return None


def portfolio_value(portfolio):
    return sum(money(p.get("amount", 0)) for p in portfolio)


# ---------------- محرك القرار ----------------
BUY_ACTIONS = {"buy", "accumulate", "شراء", "تراكم"}
KEEP_ACTIONS = {"hold", "احتفاظ", "الاحتفاظ"}
SELL_ACTIONS = {"reduce", "avoid", "تقليل", "تجنب"}


def normalize_action(action):
    if not action:
        return "hold"
    a = str(action).strip().lower()
    if a in ("buy", "accumulate", "شراء", "تراكم", "تراكمي"):
        return "buy"
    if a in ("hold", "احتفاظ", "الاحتفاظ", "ثبات"):
        return "hold"
    if a in ("reduce", "avoid", "تقليل", "تجنب", "بيع", "تخفيض"):
        return "sell"
    return "hold"


def align_state(action):
    if action == "buy":
        return "aligned"
    if action == "sell":
        return "conflict"
    return "watch"


def build_decisions(strategy):
    """توليد الأوامر التنفيذية من الاستراتيجية مقارنة بالمحفظة الحالية."""
    companies = strategy.get("companies", []) or []
    portfolio = get_portfolio()
    decisions = []

    for rec in companies:
        company = rec.get("company") or rec.get("name")
        if not company:
            continue
        action = normalize_action(rec.get("action"))
        weight = float(rec.get("weight_pct") or 0)
        target_amount = money(rec.get("amount") or (weight / 100.0 * money(strategy.get("capital"))))
        confidence = float(rec.get("confidence") or 0)
        reason = rec.get("reason_title") or rec.get("reason_source") or ""
        pos = portfolio_lookup(portfolio, company)
        pos_amount = money(pos.get("amount")) if pos else 0

        if action == "buy":
            if pos is None:
                if target_amount > 0:
                    decisions.append({
                        "id": gen_id(),
                        "company": company,
                        "sector": rec.get("sector", ""),
                        "side": "buy",
                        "amount": target_amount,
                        "weight_pct": weight,
                        "confidence": confidence,
                        "reason": reason or "توصية شراء من الاستراتيجية",
                        "status": "pending",
                        "type": "شراء جديد",
                        "alignment": "aligned",
                        "created_at": now_iso(),
                    })
            else:
                if target_amount > pos_amount * 1.15:
                    decisions.append({
                        "id": gen_id(),
                        "company": company,
                        "sector": rec.get("sector", ""),
                        "side": "buy",
                        "amount": target_amount - pos_amount,
                        "weight_pct": weight,
                        "confidence": confidence,
                        "reason": reason or "الوزن المستهدف أعلى من الحيازة الحالية",
                        "status": "pending",
                        "type": "زيادة الحيازة",
                        "alignment": "aligned",
                        "created_at": now_iso(),
                    })
                else:
                    decisions.append({
                        "id": gen_id(),
                        "company": company,
                        "sector": rec.get("sector", ""),
                        "side": "hold",
                        "amount": pos_amount,
                        "weight_pct": weight,
                        "confidence": confidence,
                        "reason": "الحيازة متوافقة مع الاستراتيجية",
                        "status": "kept",
                        "type": "إبقاء (متوافق)",
                        "alignment": "aligned",
                        "created_at": now_iso(),
                    })
        elif action == "hold":
            if pos:
                decisions.append({
                    "id": gen_id(),
                    "company": company,
                    "sector": rec.get("sector", ""),
                    "side": "hold",
                    "amount": pos_amount,
                    "weight_pct": weight,
                    "confidence": confidence,
                    "reason": "الاحتفاظ ضمن توصيات الاستراتيجية",
                    "status": "kept",
                    "type": "إبقاء",
                    "alignment": "aligned",
                    "created_at": now_iso(),
                })
            else:
                decisions.append({
                    "id": gen_id(),
                    "company": company,
                    "sector": rec.get("sector", ""),
                    "side": "watch",
                    "amount": 0,
                    "weight_pct": weight,
                    "confidence": confidence,
                    "reason": "مراقبة بدون حيازة حالية",
                    "status": "watch",
                    "type": "مراقبة",
                    "alignment": "watch",
                    "created_at": now_iso(),
                })
        else:  # sell
            if pos:
                pct = 0.5 if normalize_action(rec.get("action")) == "sell" and str(rec.get("action", "")).strip() not in ("تجنب", "avoid") else 1.0
                decisions.append({
                    "id": gen_id(),
                    "company": company,
                    "sector": rec.get("sector", ""),
                    "side": "sell",
                    "amount": money(pos_amount * pct),
                    "weight_pct": weight,
                    "confidence": confidence,
                    "reason": reason or "تعارض مع توصيات الاستراتيجية الحالية",
                    "status": "pending",
                    "type": "بيع كامل" if pct >= 1 else "تخفيض",
                    "alignment": "conflict",
                    "created_at": now_iso(),
                })
            else:
                decisions.append({
                    "id": gen_id(),
                    "company": company,
                    "sector": rec.get("sector", ""),
                    "side": "watch",
                    "amount": 0,
                    "weight_pct": weight,
                    "confidence": confidence,
                    "reason": "لا حيازة، لا إجراء",
                    "status": "watch",
                    "type": "مراقبة",
                    "alignment": "watch",
                    "created_at": now_iso(),
                })

    # فحص حيازات لا تظهر في توصيات الاستراتيجية (خرجت من التغطية)
    rec_companies = {c.get("company") or c.get("name") for c in companies}
    for pos in portfolio:
        if pos.get("company") not in rec_companies:
            decisions.append({
                "id": gen_id(),
                "company": pos["company"],
                "sector": pos.get("sector", ""),
                "side": "sell",
                "amount": money(pos.get("amount", 0)),
                "weight_pct": 0,
                "confidence": 0,
                "reason": "الشركة خرجت من توصيات الاستراتيجية — يُراجَع أو يُباع",
                "status": "pending",
                "type": "مراجعة/بيع",
                "alignment": "conflict",
                "created_at": now_iso(),
            })

    return decisions


def gen_id():
    import uuid
    return uuid.uuid4().hex[:10]


# ---------------- تنفيذ ----------------
def executor_side(side):
    return "BUY" if side == "buy" else "SELL" if side == "sell" else "HOLD"


def send_order_to_executor(order, settings):
    url = settings.get("executor_webhook") or ""
    if not url:
        return None, "لا يوجد منفذ تنفيذ (webhook) مكوَّن"
    payload = {
        "side": executor_side(order["side"]),
        "company": order["company"],
        "amount": order["amount"],
        "reference": order["id"],
    }
    code, body = http_get(url, timeout=20, data=json.dumps(payload).encode("utf-8"),
                          headers={"Content-Type": "application/json"})
    if code == 0:
        return None, f"تعذر الاتصال بمنفذ التنفيذ: {body}"
    return code, body[:200]


def apply_order_to_portfolio(order):
    with _portfolio_lock:
        portfolio = get_portfolio()
        if order["side"] == "buy":
            pos = portfolio_lookup(portfolio, order["company"])
            if pos:
                pos["amount"] = money(pos.get("amount", 0) + order["amount"])
                pos["updated_at"] = now_iso()
            else:
                portfolio.append({
                    "company": order["company"],
                    "sector": order.get("sector", ""),
                    "amount": order["amount"],
                    "shares": None,
                    "avg_price": None,
                    "added_at": now_iso(),
                    "updated_at": now_iso(),
                })
        elif order["side"] == "sell":
            pos = portfolio_lookup(portfolio, order["company"])
            if pos:
                remaining = money(pos.get("amount", 0) - order["amount"])
                if remaining <= 0:
                    portfolio = [p for p in portfolio if p.get("company") != order["company"]]
                else:
                    pos["amount"] = remaining
                    pos["updated_at"] = now_iso()
        save_portfolio(portfolio)


def execute_orders(decisions, force=False):
    settings = get_settings()
    session = get_session()
    auto = settings.get("auto_execute") or force
    with _orders_lock:
        orders = get_orders()
    # إزالة أي أوامر بانتظار قديمة لنفس الشركة (تقرير الدورة الأحدث يسود)
    current_companies = {d["company"] for d in decisions if d["side"] in ("buy", "sell")}
    orders = [o for o in orders if not (o.get("status") == "pending" and o.get("company") in current_companies)]
    executed = []
    for d in decisions:
        if d["side"] in ("buy", "sell"):
            if auto:
                if session.get("state") != "active":
                    _state["executor_error"] = "لا يمكن التنفيذ قبل تسجيل الدخول إلى المنصة"
                    continue
                code, msg = send_order_to_executor(d, settings)
                if code is None:
                    _state["executor_error"] = msg
                    continue
                d["status"] = "executed"
                d["executed_at"] = now_iso()
                d["executor_response"] = msg
                d["result"] = code
                apply_order_to_portfolio(d)
                executed.append(d)
                _state["executor_error"] = None
            else:
                d["status"] = "pending"
                orders.append(d)
        else:
            d["status"] = "kept" if d.get("status") in ("kept",) else "watch"
    with _orders_lock:
        save_orders(orders)
    if executed:
        history = get_history()
        for e in executed:
            history = [h for h in history if h.get("id") != e["id"]]
        history.extend(executed)
        save_history(history[-100:])
    return executed


# ---------------- تقارير دورية ----------------
def build_report(strategy, decisions, note=""):
    portfolio = get_portfolio()
    orders = get_orders()
    session = get_session()
    settings = get_settings()
    market = (strategy or {}).get("market", {}) or {}
    allocation = (strategy or {}).get("allocation", {}) or {}
    aligned = [d for d in decisions if d.get("alignment") == "aligned"]
    conflicted = [d for d in decisions if d.get("alignment") == "conflict"]
    buys = [d for d in decisions if d["side"] == "buy" and d.get("status") == "pending"]
    sells = [d for d in decisions if d["side"] == "sell" and d.get("status") == "pending"]
    value = portfolio_value(portfolio)
    lines = [
        f"قيمة المحفظة المستثمرة: {money(value):,} ر.س عبر {len(portfolio)} حيازة.",
        f"التوافق مع الاستراتيجية: {len(aligned)} توصية متوافقة، {len(conflicted)} تعارض أو مراجعة.",
        f"أوامر بانتظار التنفيذ: {len(buys)} شراء ({money(sum(d['amount'] for d in buys)):,} ر.س) و {len(sells)} بيع ({money(sum(d['amount'] for d in sells)):,} ر.س).",
    ]
    if strategy:
        lines.append(
            f"حالة السوق: {market.get('regime', '-')}، معنويات {'إيجابية' if market.get('sentiment', 0) >= 0 else 'سلبية'} "
            f"({market.get('sentiment', 0):+d}%)، توزيع مقترح استثمار {allocation.get('invested_pct', 0)}% / سيولة {allocation.get('cash_pct', 0)}%."
        )
    if note:
        lines.append(note)
    return {
        "at": now_iso(),
        "portfolio_value": value,
        "positions_count": len(portfolio),
        "aligned_count": len(aligned),
        "conflicted_count": len(conflicted),
        "pending_buys": len(buys),
        "pending_sells": len(sells),
        "buy_amount": money(sum(d["amount"] for d in buys)),
        "sell_amount": money(sum(d["amount"] for d in sells)),
        "regime": market.get("regime", "-"),
        "sentiment": market.get("sentiment", 0),
        "session": session.get("state", "off"),
        "lines": lines,
        "note": note,
    }


def push_report(report):
    settings = get_settings()
    url = settings.get("report_webhook") or ""
    if url:
        try:
            http_get(url, timeout=15, data=json.dumps(report).encode("utf-8"),
                     headers={"Content-Type": "application/json"})
        except Exception:
            pass


# ---------------- حلقة المتابعة الدورية (24/7) ----------------
def monitor_loop():
    while True:
        try:
            strategy = fetch_strategy()
            decisions = build_decisions(strategy) if strategy else []
            executed = execute_orders(decisions)
            note = ""
            if executed:
                note = "تم تنفيذ " + "، ".join(f"{e['company']} ({e['type']})" for e in executed) + "."
            report = build_report(strategy, decisions, note=note)
            with _orders_lock:
                _state["last_report"] = report
            reports = get_reports()
            reports.append(report)
            save_reports(reports[-MAX_REPORTS:])
            push_report(report)
            with _orders_lock:
                _state["last_decision"] = {
                    "at": now_iso(),
                    "total": len(decisions),
                    "buys": sum(1 for d in decisions if d["side"] == "buy"),
                    "sells": sum(1 for d in decisions if d["side"] == "sell"),
                    "holds": sum(1 for d in decisions if d["side"] == "hold"),
                }
        except Exception as ex:
            _state["strategy_error"] = f"حلقة المتابعة: {ex}"
        time.sleep(MONITOR_INTERVAL * 60)


# ---------------- إبقاء الجلسة حية (عدم تسجيل الخروج) ----------------
def keepalive_loop():
    while True:
        try:
            session = get_session()
            if session.get("state") == "active" and session.get("url"):
                code, _ = http_get(session["url"], timeout=10)
                session["last_ping"] = now_iso()
                session["last_ping_code"] = code
                save_session(session)
        except Exception:
            pass
        time.sleep(KEEPALIVE_INTERVAL * 60)


# ---------------- HTTP ----------------
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
            if n <= 0:
                return {}
            return json.loads(self.rfile.read(n).decode("utf-8", "replace"))
        except Exception:
            return {}

    def _send(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _serve_public(self, path):
        name = "index.html" if path in ("/", "") else path
        fp = PUBLIC / name
        if not fp.exists():
            self._send(404, {"ok": False, "error": "not found"})
            return
        ctype = "text/html; charset=utf-8" if name.endswith(".html") else \
                "application/javascript; charset=utf-8" if name.endswith(".js") else \
                "text/css; charset=utf-8"
        data = fp.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parts = [p for p in self.path.split("?")[0].split("/") if p]
        if not parts:
            return self._serve_public("index.html")
        if parts[0] in ("index.html", "app.js", "style.css"):
            return self._serve_public(parts[0])
        if parts == ["api", "status"]:
            session = get_session()
            self._send(200, {
                "ok": True,
                "name": "وكيل التنفيذ الذكي",
                "version": "1.0",
                "strategy_url": DEFAULT_STRATEGY_URL,
                "strategy_connected": strategy_connected(),
                "strategy_error": _state.get("strategy_error"),
                "session": session.get("state"),
                "session_url": session.get("url", ""),
                "auto_execute": get_settings().get("auto_execute"),
                "last_decision": _state.get("last_decision"),
                "last_report": _state.get("last_report"),
                "monitor_minutes": MONITOR_INTERVAL,
                "keepalive_minutes": KEEPALIVE_INTERVAL,
            })
            return
        if parts == ["api", "strategy"]:
            with _strategy_lock:
                s = _state.get("strategy")
            if s:
                self._send(200, {"ok": True, "strategy": s,
                                 "fetched_at": _state.get("strategy_fetched_at")})
            else:
                try:
                    s = fetch_strategy()
                except Exception:
                    s = None
                if s:
                    self._send(200, {"ok": True, "strategy": s})
                else:
                    self._send(503, {"ok": False, "error": _state.get("strategy_error")})
            return
        if parts == ["api", "portfolio"]:
            self._send(200, {"ok": True, "portfolio": get_portfolio(),
                             "value": portfolio_value(get_portfolio())})
            return
        if parts == ["api", "orders"]:
            self._send(200, {"ok": True, "orders": get_orders()})
            return
        if parts == ["api", "history"]:
            self._send(200, {"ok": True, "history": get_history()})
            return
        if parts == ["api", "reports"]:
            self._send(200, {"ok": True, "reports": get_reports()})
            return
        if parts == ["api", "settings"]:
            self._send(200, {"ok": True, "settings": get_settings(),
                             "session": get_session()})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        parts = [p for p in self.path.split("?")[0].split("/") if p]
        body = self._body()
        if parts == ["api", "login"]:
            url = (body.get("url") or "").strip()
            username = (body.get("username") or "").strip()
            password = (body.get("password") or "").strip()
            if not url or not username or not password:
                self._send(400, {"ok": False, "error": "أرسل رابط المنصة واسم المستخدم وكلمة المرور"})
                return
            if not url.startswith("http"):
                url = "https://" + url
            session = get_session()
            session.update({
                "state": "awaiting_otp",
                "url": url,
                "username": username,
                "password": password,
                "otp": None,
                "last_ping": None,
                "last_ping_code": None,
                "logged_in_at": now_iso(),
            })
            save_session(session)
            # فحص إمكانية الوصول للمنصة
            code, _ = http_get(url, timeout=12)
            session = get_session()
            session["probe_code"] = code if code else None
            save_session(session)
            self._send(200, {"ok": True, "session": "awaiting_otp",
                             "message": "تم حفظ بيانات الدخول. أرسل رمز OTP الآن.",
                             "probe_code": session.get("probe_code")})
            return
        if parts == ["api", "otp"]:
            otp = (body.get("otp") or "").strip()
            session = get_session()
            if session.get("state") != "awaiting_otp":
                self._send(400, {"ok": False, "error": "لا يوجد طلب تسجيل دخول قيد الانتظار"})
                return
            if not otp:
                self._send(400, {"ok": False, "error": "أرسل رمز OTP"})
                return
            session["otp"] = otp
            session["state"] = "active"
            session["last_ping"] = now_iso()
            session["verified_at"] = now_iso()
            save_session(session)
            self._send(200, {"ok": True, "session": "active",
                             "message": "تم الاتصال بالمنصة. الجلسة نشطة وسيبقيها الوكيل حية دون تسجيل خروج."})
            return
        if parts == ["api", "logout"]:
            session = get_session()
            url = session.get("url", "")
            session.update({"state": "off", "otp": None, "password": "",
                            "logged_out_at": now_iso()})
            save_session(session)
            note = "تم تسجيل الخروج من الجلسة المحلية."
            if url:
                # محاولة استدعاء إنهاء الجلسة على المنصة إن وجد
                try:
                    code, _ = http_get(url.rstrip("/") + "/logout", timeout=10)
                    note += f" إشعار خروج للمنصة (HTTP {code})."
                except Exception:
                    pass
            self._send(200, {"ok": True, "message": note})
            return
        if parts == ["api", "confirm"]:
            oid = (body.get("id") or "").strip()
            with _orders_lock:
                orders = get_orders()
                order = next((o for o in orders if o.get("id") == oid), None)
                if not order:
                    self._send(404, {"ok": False, "error": "الأمر غير موجود"})
                    return
                if order["side"] not in ("buy", "sell"):
                    self._send(400, {"ok": False, "error": "لا يمكن تأكيد أمر مراقبة"})
                    return
                orders = [o for o in orders if o.get("id") != oid]
                save_orders(orders)
            order["status"] = "executed"
            order["executed_at"] = now_iso()
            order["result"] = "manual"
            apply_order_to_portfolio(order)
            history = get_history()
            history.append(order)
            save_history(history[-100:])
            self._send(200, {"ok": True, "message": f"تم تنفيذ {order['company']} يدوياً"})
            return
        if parts == ["api", "cancel"]:
            oid = (body.get("id") or "").strip()
            with _orders_lock:
                orders = get_orders()
                order = next((o for o in orders if o.get("id") == oid), None)
                if not order:
                    self._send(404, {"ok": False, "error": "الأمر غير موجود"})
                    return
                orders = [o for o in orders if o.get("id") != oid]
                save_orders(orders)
            order["status"] = "cancelled"
            order["cancelled_at"] = now_iso()
            history = get_history()
            history.append(order)
            save_history(history[-100:])
            self._send(200, {"ok": True, "message": f"تم إلغاء أمر {order['company']}"})
            return
        if parts == ["api", "settings"]:
            s = get_settings()
            if "auto_execute" in body:
                s["auto_execute"] = bool(body["auto_execute"])
            if "executor_webhook" in body:
                s["executor_webhook"] = (body["executor_webhook"] or "").strip()
            if "report_webhook" in body:
                s["report_webhook"] = (body["report_webhook"] or "").strip()
            if "confirm_orders" in body:
                s["confirm_orders"] = bool(body["confirm_orders"])
            save_settings(s)
            self._send(200, {"ok": True, "settings": s})
            return
        if parts == ["api", "run"]:
            # تنفيذ فوري لدورة واحدة
            if not _loop_lock.acquire(blocking=False):
                self._send(429, {"ok": False, "error": "دورة قيد التشغيل حالياً"})
                return
            try:
                strategy = fetch_strategy()
                decisions = build_decisions(strategy) if strategy else []
                executed = execute_orders(decisions)
                note = ""
                if executed:
                    note = "تم تنفيذ " + "، ".join(f"{e['company']} ({e['type']})" for e in executed) + "."
                report = build_report(strategy, decisions, note=note)
                reports = get_reports()
                reports.append(report)
                save_reports(reports[-MAX_REPORTS:])
                push_report(report)
                _state["last_report"] = report
                _state["last_decision"] = {
                    "at": now_iso(),
                    "total": len(decisions),
                    "buys": sum(1 for d in decisions if d["side"] == "buy"),
                    "sells": sum(1 for d in decisions if d["side"] == "sell"),
                    "holds": sum(1 for d in decisions if d["side"] == "hold"),
                }
                self._send(200, {"ok": True, "report": report,
                                 "decisions": len(decisions), "executed": len(executed)})
            finally:
                _loop_lock.release()
            return
        if parts == ["api", "clear"]:
            for f in (PORTFOLIO_FILE, ORDERS_FILE, SESSION_FILE, REPORTS_FILE, HISTORY_FILE):
                if f.exists():
                    f.unlink()
            session = get_session()
            session.update({"state": "off"})
            save_session(session)
            self._send(200, {"ok": True, "message": "تم مسح المحفظة والأوامر والجلسة"})
            return
        self._send(404, {"ok": False, "error": "not found"})


def main():
    port = int(os.environ.get("PORT", DEFAULT_PORT))
    save_json(PORT_FILE, port)
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=keepalive_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    srv.serve_forever()


if __name__ == "__main__":
    main()
