import csv
import io
import json
import math
import os
import re
import calendar
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv
from snapi_py_client.snapi_bridge import StocknoteAPIPythonBridge as Samco

# ======= LOAD ENV =======
load_dotenv()
SAMCO_USER_ID = os.getenv("SAMCO_USER_ID")
SAMCO_PASSWORD = os.getenv("SAMCO_PASSWORD")
SAMCO_YOB = os.getenv("SAMCO_YOB")

# ======= CONFIG =======
OUTPUT_JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output.json")

# Index symbols we will treat as "indices"
INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYIT", "NIFTYFINSERVICE"}

# Index name mapping for Samco index_quote() API
INDEX_NAME_MAP = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
    "FINNIFTY": "NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NIFTY MID SELECT",
    "NIFTYIT": "NIFTY IT",
    "NIFTYFINSERVICE": "NIFTY FIN SERVICE",
}

# NSE endpoints (still used for ban list, MWPL, holidays — Samco doesn't provide these)
NSE_BAN_CSV_URL   = "https://nsearchives.nseindia.com/content/fo/fo_secban.csv"
NSE_HOLIDAYS_URL  = "https://www.nseindia.com/api/holiday-master?type=trading"
NSE_LOT_SIZE_URL  = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"

# Expiry changeover: last Thursday -> last Tuesday
CHANGEOVER_DATE = date(2025, 9, 1)


# ======= Helpers =======
def safe_float(val) -> float:
    """Parse a float from Samco responses that may have commas or be None."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


# ======= Samco Client =======
_samco: Optional[Samco] = None

def get_samco() -> Samco:
    """Login to Samco and return the authenticated client. Reuses existing session."""
    global _samco
    if _samco is not None:
        return _samco

    if not all([SAMCO_USER_ID, SAMCO_PASSWORD, SAMCO_YOB]):
        raise RuntimeError("Samco credentials not found. Set SAMCO_USER_ID, SAMCO_PASSWORD, SAMCO_YOB in .env")

    print("[SAMCO] Logging in...")
    samco = Samco()
    login_resp = samco.login(body={
        "userId": SAMCO_USER_ID,
        "password": SAMCO_PASSWORD,
        "yob": SAMCO_YOB,
    })
    if isinstance(login_resp, str):
        login_resp = json.loads(login_resp)

    if login_resp.get("status") != "Success":
        raise RuntimeError(f"Samco login failed: {login_resp.get('statusMessage', login_resp)}")

    session_token = login_resp.get("sessionToken")
    if not session_token:
        raise RuntimeError("No session token in login response")

    samco.set_session_token(sessionToken=session_token)
    print(f"[SAMCO] Login successful! Account: {login_resp.get('accountName', 'N/A')}")
    _samco = samco
    return _samco


# ======= NSE HTTP helper (only for ban list / holidays) =======
_nse_session: Optional[requests.Session] = None

def _get_nse_session() -> requests.Session:
    global _nse_session
    if _nse_session is not None:
        return _nse_session
    s = requests.Session()
    s.headers.update({
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'accept-language': 'en',
    })
    try:
        s.get("https://www.nseindia.com/option-chain", timeout=10)
    except Exception:
        pass
    _nse_session = s
    return _nse_session


# ======= Holidays =======
_holidays_cache: Optional[Set[date]] = None

def parse_date_flex(s: str) -> Optional[date]:
    s = (s or "").strip()
    for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    m = re.search(r"(\d{1,2})[-/\s]([A-Za-z]{3})[-/\s](\d{4})", s)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)}-{m.group(2)}-{m.group(3)}", "%d-%b-%Y").date()
        except Exception:
            return None
    return None

def get_trading_holidays() -> Set[date]:
    global _holidays_cache
    if _holidays_cache is not None:
        return _holidays_cache
    try:
        s = _get_nse_session()
        j = s.get(NSE_HOLIDAYS_URL, timeout=10).json()
        hols: Set[date] = set()
        for seg_key in ("FO", "TRADING", "CM"):
            for item in j.get(seg_key, []):
                dt_txt = item.get("tradingDate") or item.get("date")
                d = parse_date_flex(dt_txt)
                if d:
                    hols.add(d)
        _holidays_cache = hols
        return hols
    except Exception:
        _holidays_cache = set()
        return _holidays_cache

# ======= Expiry helpers =======
def _last_weekday_of_month(d: date, weekday: int) -> date:
    last_day = calendar.monthrange(d.year, d.month)[1]
    last_dt = date(d.year, d.month, last_day)
    delta = (last_dt.weekday() - weekday) % 7
    return last_dt - timedelta(days=delta)

def _prev_trading_day(dt: date, holidays: Set[date]) -> date:
    cur = dt
    while cur.weekday() >= 5 or cur in holidays:
        cur -= timedelta(days=1)
    return cur

def last_thursday_monthly(d: date, holidays: Set[date]) -> date:
    return _prev_trading_day(_last_weekday_of_month(d, 3), holidays)

def last_tuesday_monthly(d: date, holidays: Set[date]) -> date:
    return _prev_trading_day(_last_weekday_of_month(d, 1), holidays)

def monthly_expiry_for_series(today: date) -> date:
    holidays = get_trading_holidays()
    th_this = last_thursday_monthly(today, holidays)
    series_month = today if today <= th_this else date(today.year + (1 if today.month == 12 else 0),
                                                       1 if today.month == 12 else today.month + 1, 1)
    th_exp = last_thursday_monthly(series_month, holidays)
    tu_exp = last_tuesday_monthly(series_month, holidays)
    use_tuesday = (tu_exp >= CHANGEOVER_DATE)
    expiry = tu_exp if use_tuesday else th_exp
    if expiry < today:
        nm = date(series_month.year + (1 if series_month.month == 12 else 0),
                  1 if series_month.month == 12 else series_month.month + 1, 1)
        th_exp = last_thursday_monthly(nm, holidays)
        tu_exp = last_tuesday_monthly(nm, holidays)
        use_tuesday = (tu_exp >= CHANGEOVER_DATE)
        expiry = tu_exp if use_tuesday else th_exp
    return expiry

def format_expiry_samco(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def format_ddMONyyyy(d: date) -> str:
    return d.strftime("%d-%b-%Y")


# ======= Samco data fetching =======
@dataclass
class ChainPick:
    spot: float
    atm_strike: int
    ce_ltp: Optional[float]
    pe_ltp: Optional[float]
    used_expiry_text: str
    ce_trading_symbol: Optional[str] = None
    pe_trading_symbol: Optional[str] = None

def is_index_symbol(sym: str) -> bool:
    return sym.upper() in INDEX_SYMBOLS


def get_spot_price(samco: Samco, sym: str, is_index: bool) -> float:
    """Get spot/closing price. Falls back to closeValue/previousClose after hours."""
    try:
        if is_index:
            index_name = INDEX_NAME_MAP.get(sym.upper(), sym)
            resp = samco.index_quote(index_name)
            if isinstance(resp, str):
                resp = json.loads(resp)
            if resp.get("status") == "Success":
                details = resp.get("indexDetails", [])
                if details:
                    d = details[0]
                    for field in ("spotPrice", "lastTradedPrice", "closePrice", "previousClose"):
                        v = safe_float(d.get(field))
                        if v > 0:
                            return v
        else:
            resp = samco.get_quote(sym, exchange="NSE")
            if isinstance(resp, str):
                resp = json.loads(resp)
            if resp.get("status") == "Success":
                # Data is nested under 'quoteDetails'
                qd = resp.get("quoteDetails", resp)
                for field in ("lastTradedPrice", "closeValue", "previousClose"):
                    v = safe_float(qd.get(field))
                    if v > 0:
                        return v
    except Exception as e:
        print(f"[SPOT PRICE ERROR] {sym}: {e}")
    return 0.0


def fetch_option_chain_samco(samco: Samco, sym: str, is_index: bool, expiry_date_str: str) -> List[dict]:
    try:
        resp = samco.get_option_chain(
            search_symbol_name=sym,
            exchange="NFO",
            expiry_date=expiry_date_str,
        )
        if isinstance(resp, str):
            resp = json.loads(resp)
        if resp.get("status") == "Success":
            return resp.get("optionChainDetails", [])
        else:
            print(f"[OPTION CHAIN WARNING] {sym}: {resp.get('statusMessage', 'Unknown error')}")
            return []
    except Exception as e:
        print(f"[OPTION CHAIN ERROR] {sym}: {e}")
        return []


def find_atm_straddle(samco: Samco, sym: str, is_index: bool, expiry_date_str: str) -> ChainPick:
    spot = get_spot_price(samco, sym, is_index)
    print(f"[SAMCO] {sym} spot price: {spot}")

    if spot <= 0:
        return ChainPick(spot=0, atm_strike=0, ce_ltp=None, pe_ltp=None, used_expiry_text=expiry_date_str)

    chain = fetch_option_chain_samco(samco, sym, is_index, expiry_date_str)
    if not chain:
        return ChainPick(spot=spot, atm_strike=0, ce_ltp=None, pe_ltp=None, used_expiry_text=expiry_date_str)

    # Also try to get spot from the chain items themselves (they have spotPrice)
    if spot <= 0 and chain:
        for item in chain:
            sp = safe_float(item.get("spotPrice"))
            if sp > 0:
                spot = sp
                print(f"[SAMCO] {sym} spot from option chain: {spot}")
                break

    if spot <= 0:
        return ChainPick(spot=0, atm_strike=0, ce_ltp=None, pe_ltp=None, used_expiry_text=expiry_date_str)

    # Parse strikes
    strikes_data: Dict[float, dict] = {}
    for item in chain:
        strike = safe_float(item.get("strikePrice", 0))
        opt_type = item.get("optionType", "").upper()
        trading_sym = item.get("tradingSymbol", "")

        # Use lastTradedPrice first, fallback to previousClosePrice (for after hours)
        ltp = safe_float(item.get("lastTradedPrice"))
        if ltp <= 0:
            ltp = safe_float(item.get("previousClosePrice"))

        if strike not in strikes_data:
            strikes_data[strike] = {"ce_ltp": None, "pe_ltp": None, "ce_sym": None, "pe_sym": None}

        if opt_type == "CE":
            strikes_data[strike]["ce_ltp"] = ltp
            strikes_data[strike]["ce_sym"] = trading_sym
        elif opt_type == "PE":
            strikes_data[strike]["pe_ltp"] = ltp
            strikes_data[strike]["pe_sym"] = trading_sym

    # Find ATM
    valid_strikes = [s for s, d in strikes_data.items()
                     if d["ce_ltp"] and d["pe_ltp"] and d["ce_ltp"] > 0 and d["pe_ltp"] > 0]

    if not valid_strikes:
        all_strikes = list(strikes_data.keys())
        if all_strikes:
            closest = min(all_strikes, key=lambda s: abs(s - spot))
            return ChainPick(
                spot=spot, atm_strike=int(closest),
                ce_ltp=strikes_data[closest].get("ce_ltp"),
                pe_ltp=strikes_data[closest].get("pe_ltp"),
                used_expiry_text=expiry_date_str,
                ce_trading_symbol=strikes_data[closest].get("ce_sym"),
                pe_trading_symbol=strikes_data[closest].get("pe_sym"),
            )
        return ChainPick(spot=spot, atm_strike=0, ce_ltp=None, pe_ltp=None, used_expiry_text=expiry_date_str)

    # Prefer CE <= PE, minimize diff
    spot_range = spot * 0.2
    nearby = [s for s in valid_strikes if abs(s - spot) <= spot_range] or valid_strikes

    best_strike = None
    best_score = float('inf')
    for strike in nearby:
        ce_p = strikes_data[strike]["ce_ltp"]
        pe_p = strikes_data[strike]["pe_ltp"]
        score = abs(pe_p - ce_p) + (0 if ce_p <= pe_p else 100) + abs(strike - spot) * 0.01
        if score < best_score:
            best_score = score
            best_strike = strike

    if best_strike is None:
        best_strike = min(nearby, key=lambda x: abs(x - spot))

    ce_ltp = strikes_data[best_strike]["ce_ltp"]
    pe_ltp = strikes_data[best_strike]["pe_ltp"]
    print(f"[STRIKE SELECTION] {sym} | Spot: {spot} | Strike: {best_strike} | CE: {ce_ltp} | PE: {pe_ltp} | Straddle: {ce_ltp + pe_ltp}")
    return ChainPick(
        spot=spot, atm_strike=int(best_strike),
        ce_ltp=ce_ltp, pe_ltp=pe_ltp,
        used_expiry_text=expiry_date_str,
        ce_trading_symbol=strikes_data[best_strike].get("ce_sym"),
        pe_trading_symbol=strikes_data[best_strike].get("pe_sym"),
    )


# ======= SPAN Margin via Samco =======
def fetch_span_margin(samco: Samco, ce_trading_sym: str, pe_trading_sym: str,
                      lot_size: int) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not ce_trading_sym or not pe_trading_sym or not lot_size:
        return None, None, None
    try:
        resp = samco.span_margin(body={
            "request": [
                {"exchange": "NFO", "tradingSymbol": ce_trading_sym, "qty": str(lot_size)},
                {"exchange": "NFO", "tradingSymbol": pe_trading_sym, "qty": str(lot_size)},
            ]
        })
        if isinstance(resp, str):
            resp = json.loads(resp)
        if resp.get("status") == "Success":
            details = resp.get("spanDetails", {})
            span_req = float(details.get("spanRequirement", 0))
            exposure = float(details.get("exposureMargin", 0))
            total = float(details.get("totalRequirement", 0))
            print(f"[SPAN MARGIN] SPAN: {span_req} | Exposure: {exposure} | Total: {total}")
            return span_req, exposure, total
        else:
            print(f"[SPAN MARGIN WARNING] {resp.get('statusMessage', 'Unknown')}")
    except Exception as e:
        print(f"[SPAN MARGIN ERROR] {e}")
    return None, None, None


# ======= Fallback margin estimation =======
def _default_margin_pct(sym: str, is_index: bool) -> Tuple[float, float]:
    if is_index:
        return 0.10, 0.03
    else:
        return 0.139, 0.07

def compute_fallback_margin(contract_value: Optional[float], sym: str,
                            is_index: bool, span_pct_override=None,
                            exposure_pct_override=None) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not contract_value:
        return None, None, None
    d_span, d_exp = _default_margin_pct(sym, is_index)
    span_pct = span_pct_override if span_pct_override is not None else d_span
    exp_pct = exposure_pct_override if exposure_pct_override is not None else d_exp
    span_r = round(contract_value * span_pct, 2)
    exp_r  = round(contract_value * exp_pct, 2)
    total  = round(span_r + exp_r, 2)
    return span_r, exp_r, total


# ======= Ban list + MWPL (still from NSE) =======
def fetch_official_ban_symbols() -> Set[str]:
    try:
        s = _get_nse_session()
        r = s.get(NSE_BAN_CSV_URL, timeout=10)
        r.raise_for_status()
        data = r.content.decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(data))
        out: Set[str] = set()
        for row in reader:
            if len(row) >= 2 and row[0].strip().isdigit():
                out.add(row[1].strip().upper())
        return out
    except Exception:
        return set()

def fetch_public_mwpl_snapshot() -> Dict[str, float]:
    UA = {"User-Agent": "Mozilla/5.0"}
    out: Dict[str, float] = {}
    def parse_pairs(html: str):
        for m in re.finditer(r">([A-Z0-9]{2,})</td>\s*<td[^>]*>\s*([7-9]\d(?:\.\d+)?)\s*%</td>", html, flags=re.I):
            sym = m.group(1).upper().strip()
            pct = float(m.group(2))
            out[sym] = max(out.get(sym, 0.0), pct)
    try:
        html = requests.get("https://www.5paisa.com/nse-ban-list", headers=UA, timeout=10).text
        parse_pairs(html)
    except Exception:
        pass
    try:
        html = requests.get("https://www.niftytrader.in/ban-list", headers=UA, timeout=10).text
        parse_pairs(html)
    except Exception:
        pass
    return out


# ======= NSE official lot sizes =======
def fetch_nse_lot_sizes() -> Dict[str, int]:
    """Fetch official F&O lot sizes from NSE's fo_mktlots.csv.
    Returns dict mapping SYMBOL -> current lot size.
    """
    try:
        r = requests.get(NSE_LOT_SIZE_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        r.raise_for_status()
        lines = r.text.strip().split("\n")
        lot_map: Dict[str, int] = {}
        for line in lines[1:]:  # skip header
            cols = [c.strip() for c in line.split(",")]
            if len(cols) < 3:
                continue
            symbol = cols[1].strip().upper()
            # Skip header-like rows
            if symbol in ("", "SYMBOL"):
                continue
            # Use the first month column (current month lot size)
            lot_str = cols[2].strip()
            if lot_str and lot_str.isdigit():
                lot_map[symbol] = int(lot_str)
        print(f"[NSE LOT SIZES] Fetched {len(lot_map)} symbols")
        return lot_map
    except Exception as e:
        print(f"[NSE LOT SIZE ERROR] {e}")
        return {}


# ======= Main update — writes to JSON =======
def update_json():
    samco = get_samco()

    banned = fetch_official_ban_symbols()
    mwpl_map = fetch_public_mwpl_snapshot()
    nse_lots = fetch_nse_lot_sizes()

    if not nse_lots:
        print("[ERROR] Could not fetch F&O lot sizes from NSE. Aborting.")
        return

    # The stock list IS the NSE lot sizes dict — every F&O stock, no manual list needed
    all_fo_symbols = sorted(nse_lots.keys())
    print(f"[INFO] Processing {len(all_fo_symbols)} F&O stocks from NSE")

    now = datetime.now()
    desired_expiry = monthly_expiry_for_series(now.date())
    desired_expiry_samco = format_expiry_samco(desired_expiry)
    desired_expiry_display = format_ddMONyyyy(desired_expiry)

    results = []
    for sym in all_fo_symbols:
        lot_size = nse_lots[sym]
        is_idx = is_index_symbol(sym)

        try:
            pick = find_atm_straddle(samco, sym, is_idx, desired_expiry_samco)

            ce = pick.ce_ltp
            pe = pick.pe_ltp
            straddle = (ce + pe) if (ce is not None and pe is not None) else None

            # Margin: try Samco SPAN first, fallback to estimate
            span_r, exp_r, init_margin_r = None, None, None
            margin_source = "N/A"

            if pick.ce_trading_symbol and pick.pe_trading_symbol and lot_size:
                span_r, exp_r, init_margin_r = fetch_span_margin(
                    samco, pick.ce_trading_symbol, pick.pe_trading_symbol, lot_size
                )
                if init_margin_r is not None:
                    margin_source = "Samco SPAN API"

            if init_margin_r is None and pick.atm_strike and lot_size:
                contract_value = pick.atm_strike * lot_size
                span_r, exp_r, init_margin_r = compute_fallback_margin(
                    contract_value, sym, is_idx
                )
                if init_margin_r is not None:
                    margin_source = "Estimated (%)"

            # Premiums
            current_premium = round(straddle * lot_size, 2) if (straddle and lot_size) else None
            margin_util_pct = round((current_premium / init_margin_r) * 100, 2) if (current_premium and init_margin_r) else None
            range_pct = round(abs((pick.atm_strike - pick.spot) / pick.spot) * 100, 2) if pick.spot else None

            # Console
            print(f"[{sym}] Spot: {pick.spot} | Strike: {pick.atm_strike} | Straddle: {round(straddle, 2) if straddle else 'N/A'} | Lot: {lot_size} | Margin: {init_margin_r} ({margin_source})")

            results.append({
                "symbol": sym,
                "live_price": round(pick.spot, 2) if pick.spot else None,
                "atm_strike": pick.atm_strike,
                "ce_ltp": round(ce, 2) if ce else None,
                "pe_ltp": round(pe, 2) if pe else None,
                "straddle_price": round(straddle, 2) if straddle else None,
                "lot_size": lot_size,
                "ce_trading_symbol": pick.ce_trading_symbol,
                "pe_trading_symbol": pick.pe_trading_symbol,
                "span_margin": span_r,
                "exposure_margin": exp_r,
                "total_margin": init_margin_r,
                "margin_source": margin_source,
                "margin_in_lakhs": round(init_margin_r / 100000, 2) if init_margin_r else None,
                "current_straddle_premium": current_premium,
                "margin_utilised_pct": margin_util_pct,
                "range_pct": range_pct,
                "expiry_used": desired_expiry_display,
                "ban_status": "BANNED" if sym in banned else "OK",
                "mwpl_pct": mwpl_map.get(sym),
                "last_updated": now.strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            results.append({
                "symbol": sym,
                "live_price": None,
                "atm_strike": None,
                "straddle_price": None,
                "expiry_used": desired_expiry_display,
                "ban_status": "ERROR",
                "error": str(e)[:100],
                "last_updated": now.strftime("%Y-%m-%d %H:%M:%S"),
            })
            print(f"[UPDATE ERROR] {sym}: {str(e)[:80]}")

        time.sleep(0.3)

    # Write output
    output = {
        "run_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "desired_expiry": desired_expiry_display,
        "symbols": results,
    }
    with open(OUTPUT_JSON_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n[DONE] Output written to {OUTPUT_JSON_PATH}")


# ======= entrypoint =======
if __name__ == "__main__":
    print("=" * 70)
    print("TESTING SAMCO API")
    print("=" * 70)
    try:
        samco = get_samco()

        print("\n--- Equity quote (RELIANCE) ---")
        resp = samco.get_quote("RELIANCE", exchange="NSE")
        if isinstance(resp, str):
            resp = json.loads(resp)
        qd = resp.get("quoteDetails", {})
        print(f"Status: {resp.get('status')} | LTP: {qd.get('lastTradedPrice')} | Close: {qd.get('closeValue')}")

        desired_expiry = monthly_expiry_for_series(date.today())
        expiry_str = format_expiry_samco(desired_expiry)
        print(f"\n--- Option chain (RELIANCE, expiry={expiry_str}) ---")
        pick = find_atm_straddle(samco, "RELIANCE", False, expiry_str)
        print(f"Spot: {pick.spot} | ATM: {pick.atm_strike} | CE: {pick.ce_ltp} | PE: {pick.pe_ltp}")
        if pick.ce_ltp and pick.pe_ltp:
            print(f"Straddle: {pick.ce_ltp + pick.pe_ltp}")

        if pick.ce_trading_symbol and pick.pe_trading_symbol:
            print(f"\n--- SPAN margin test ---")
            s, e, t = fetch_span_margin(samco, pick.ce_trading_symbol, pick.pe_trading_symbol, 250)
            print(f"SPAN: {s} | Exposure: {e} | Total: {t}")

    except Exception as e:
        print(f"[TEST ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 70)
    print("RUNNING FULL UPDATE")
    print("=" * 70)
    update_json()
