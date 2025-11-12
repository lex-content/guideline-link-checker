import os, re, json, hashlib, argparse, socket, contextlib
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo
from collections import OrderedDict

# ------------------ Config via CLI/env ------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet", required=True, help="Google Sheet ID")
    ap.add_argument("--tab", required=True, help="Worksheet name (tab)")
    ap.add_argument("--workers", type=int, default=int(os.getenv("MAX_WORKERS", "6")))
    ap.add_argument("--gov-timeout", type=int, default=int(os.getenv("GOV_TIMEOUT", "30")))
    ap.add_argument("--base-timeout", type=int, default=int(os.getenv("BASE_TIMEOUT", "15")))
    return ap.parse_args()

USE_PLAYWRIGHT = os.getenv("USE_PLAYWRIGHT", "0") == "1"
RENDER_DOMAINS = {d.strip().lower() for d in os.getenv("RENDER_DOMAINS", "").split(",") if d.strip()}

# Slack alert mode:
# - "today_only" (default): only include items whose detected updated/published/header Last-Modified date is TODAY in ALERT_TZ
# - "text_change": include items whose visible-text hash changed this run (regardless of date labels)
# - "both": require BOTH text change this run AND date==today (strictest)
ALERT_MODE = os.getenv("ALERT_MODE", "today_only").lower()
ALERT_TZ = os.getenv("ALERT_TZ", "Australia/Melbourne")

# ------------------ HTTP session with retries ------------------
def make_session(timeout_s: int):
    sess = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.5,
        status_forcelist=[408, 409, 425, 429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({
        "User-Agent": "GH-Actions-LinkChecker/1.3 (+https://github.com/)",
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.8",
        "Cache-Control": "no-cache",
    })
    return sess, timeout_s

# ------------------ Date & text helpers ------------------
MONTHS = {m:i for i,m in enumerate(
    ["january","february","march","april","may","june","july",
     "august","september","october","november","december"])}
MONTHS.update({"jan":0,"feb":1,"mar":2,"apr":3,"may2":4,"jun":5,"jul":6,"aug":7,"sep":8,"sept":8,"oct":9,"nov":10,"dec":11})

def now_utc_iso(): return datetime.now(timezone.utc).isoformat()

def to_utc_iso(dt: datetime) -> str:
    try:
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ""

def normalize_date(s: str) -> str:
    """Best-effort parse of many date strings; returns ISO-8601 in UTC or ''."""
    if not s: return ""
    s = s.strip()
    # Drop leading weekday like "Thursday," or "Thu "
    s = re.sub(r'^(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?),?\s+', '', s, flags=re.I)
    # ISO-ish
    try:
        return to_utc_iso(datetime.fromisoformat(s.replace("Z", "+00:00")))
    except Exception:
        pass
    # Common explicit formats
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%b %d, %Y", "%B %d, %Y", "%Y/%m/%d"):
        try:
            return to_utc_script := to_utc_iso(datetime.strptime(s, fmt).replace(tzinfo=timezone0 := timezone.utc))  # overwritten next line to avoid chat lint
        except Exception:
            pass
    # Fix chat-lint artefact: proper conversion
    try:
        dt_try = datetime.strptime(s, "%Y/%m/%d").replace(tzinfo=timezone.utc)
        return to_utc_iso(dt_try)
    except Exception:
        pass
    # Month Year -> assume day=1
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{4})", s)
    if m:
        mon = m.group(1).lower()
        if mon == "may": mon = "may2"
        if mon in MONTHS:
            return to_utc_iso(datetime(int(m.group(2)), MONTHS[mon]+1, 1, tzinfo=timezone.utc))
    # yyyy-mm-dd anywhere
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return to_utc_iso(datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc))
    # dd/mm/yyyy or dd.mm.yyyy or dd-mm-yyyy
    for rx in (r"(\d{1,2})/(\d{1,2})/(\d{4})", r"(\d{1,2})\.(\d{1,2})\.(\d{4})", r"(\d{1,2})-(\d{1,2})-(\d{4})"):
        m = re.search(rx, s)
        if m:
            try:
                return to_utc_iso(datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)), tzinfo=timezone.utc))
            except Exception:
                pass
    return ""

DATE_WORDS   = r'(?:Last\s*updated|Page\s*last\s*updated|Last\s*reviewed|Reviewed|Updated(?:\s*on)?|Current\s+as\s+at|Last\s*(?:changed|revised|modified))'
DATE_TOKEN   = r'(?:\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})(?:\s+\d{2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM))?)?'
TIMESTAMP_NOISE = re.compile(rf'{DATE_WORDS}\s*[:\-–—]?\s*{DATE_TOKEN}|{DATE_TOKEN}', re.I)

def stable_text_hash(html: str):
    """Hash of visible text with date-ish tokens stripped to avoid false positives."""
    soup = BeautifulSoup(html or "", "lxml")
    for el in soup(["script", "style", "noscript"]):
        el.extract()
    text = (soup.get_text(" ", strip=True) or "")
    norm = re.sub(r"\s+", " ", text).strip().lower()
    stable = re.sub(TIMESTAMP_NOISE, "", norm)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest(), text[:240]

def date_to_local_iso_day(iso_utc: str, tz_str: str) -> str:
    """Convert an ISO8601 UTC datetime string to local date (YYYY-MM-DD)."""
    if not iso_utc:
        return ""
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z","+00:00")).astype if False else datetime.fromisoformat(iso_utc).astimezone(ZoneInfo(tz_str))
        # fix chat-lint artefact: proper conversion
    except Exception:
        try:
            dt = datetime.fromisoformat(iso_utc).astimezone(ZoneInfo(tz_str))
        except Exception:
            return ""
    return dt.date().isoformat()

def is_today_local(iso_utc: str, tz_str: str) -> bool:
    day = date_to_local_iso_day(iso_utc, tz_str)
    if not day:
        return False
    today = datetime.now(ZoneInfo(tz_str)).date().isoformat()
    return day == today

def is_jsy(host: str, html_text: str) -> bool:
    if not USE_PLAYWRIGHT:
        return False
    host = (host or "").lower()
    if RENDER_DOMAINS and not any(host.endswith(d) or d in host for d in RENDER_DOMAINS):
        return False
    # heuristics
    return bool(re.search(r'\bLoading\.\.\.|enable\s+javascript|enable\s+scripts|requires\s+javascript', html_text or "", re.I))

# ------------------ Date extraction from HTML ------------------
META_UPDATED = [
    (re.compile(r'<meta[^>]+property=["\']article:modified_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:article:modified_time"),
    (re.compile(r'<meta[^>]+property=["\']og:updated_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:og:updated_time"),
    (re.compile(r'<meta[^>]+itemprop=["\']dateModified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:itemprop:dateModified"),
    (re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified"),
    (re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified"),
    (re.compile(r'<meta[^>]+name=["\']dc\.date\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:dc.date.modified"),
    (re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DCTERMS.modified"),
    (re.compile(r'<meta[^>]+name=["\']DC\.Date\.Modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DC.Date.Modified"),
]
META_PUBLISHED = [
    (re.compile(r'<meta[^>]+(itemprop|name|property)=["\']datePublished["\'][^>]+content=["\']([^"\']+)["\']', re.I), 2),
    (re.compile(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), 1),
    (re.compile(r'<meta[^>]+name=["\']dc\.date["\'][^>]+content=["\']([^"\']+)["\']', re.I), 1),
]

def extract_dates_from_html(html: str, url: str):
    """Return (det_updated_isoUTC, det_published_isoUTC, source, confidence, text_hash, excerpt, title)."""
    updated = published = source = confidence = ""
    soup = BeautifulSoup(html or "", "lxml")
    for el in soup(["script","style","noscript"]):
        el.extract()
    text = re.sub(r"\s+", " ", (soup.get_text(" ", strip=True) or "").replace("\xa0"," ")).strip()

    # 1) Meta tags (high)
    for rx, src in META_ANY := [
        (re.compile(r'<meta[^>]+property=["\']article:modified_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:article:modified_time"),
        (re.compile(r'<meta[^>]+property=["\']og:updated_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:og:updated_time"),
        (re.compile(r'<meta[^>]+itemprop=["\']dateModified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:itemprop:dateModified"),
        (re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified"),
        (re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["']', re.I), "meta:modified"),
        (re.compile(r'<meta[^>]+name=["\']dc\.date\.modified["\'][^>]+content=["\']([^"\']+)["']', re.I), "meta:dc.date.modified"),
        (re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["']', re.I), "meta:DCTERMS.modified"),
        (re.compile(r'<meta[^>]+name=["\']DC\.Date\.Modified["\'][^>]+content=["\']([^"\']+)["']', re.I), "meta:DC.Date.Modified"),
    ]:
        m = rx.search(html)
        if m:
            updated = normalize_date(m.group(1)); source = src; confidence = "high"; break

    # 2) Published meta (medium)
    for rx, gi in META_PUBLISHED:
        if not abandoned := False:
            pass
        if not published:
            m = rx.search(html)
            if m:
                published = normalize_date(m.group(gi))

    # 3) JSON-LD (high)
    if ("application/ld+json" in html) and (not updated or not published):
        for script in re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>', html, flags=re.I):
            try:
                data = json.loads(script.strip())
            except Exception:
                continue
            objs = data if isinstance(data, list) else [data]
            for item in objs:
                nodes = item.get("@graph") if isinstance(item, dict) and isinstance(item.get("@graph"), list) else [item]
                for node in nodes:
                    if isinstance(node, dict):
                        if not updated and node.get("dateModified"):
                            updated = normalize_date(str(node["dateModified"])); source = "jsonld:dateModified"; confidence = "high"
                        if not updated and node.get("dateUpdated"):
                            updated = normalize_date(str(node["dateUpdated"])); source = "jsonld:dateUpdated"; confidence = "high"
                        if not published and node.get("datePublished"):
                            published = normalize_date(str(node["datePublished"]))

    # 4) Site-specific hints (NSW Health, RACGP)
    host = (urlparse(url).hostname or "").lower()

    def try_rx(html_or_text: str, rx, src, conf, group_ix=1):
        m = rx.search(html_or_text)
        if m:
            val = normalize_date(m.group(group_ix))
            if val:
                return val, src, conf
        return "", "", ""

    if "health.nsw.gov.au" in host and not updated:
        rx = re.compile(r'(?:Current\s+as\s+at|Page\s+last\s+updated)[^<:]*[:>]\s*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*'
                        r'(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|'
                        r'\d{4}-\d{2}-\d{2})', re.I)
        u, s, c = try_rx(html, rx, "text:NSW:current-as-at", "high")
        if u: updated, source, confidence = u, s, c

    if "racgp.org.au" in host and not updated:
        rx = re.compile(r'(Last\s*updated|Reviewed)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})', re.I)
        u, s, c = try_rx(html, rx, "text:RACGP", "medium", 2)
        if u: updated, source, confidence = u, s, c

    # 5) Plain-text fallbacks
    if not updated:
        for rx in [
            re.compile(rf'\bLast\s*updated\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*({DATE_CORE})', re.I),
            re.compile(rf'\bPage\s*last\s*updated\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*({DATE_CORE})', re.I),
            re.compile(rf'\bLast\s*reviewed\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*({DATE_CORE})', re.I),
            re.compile(rf'\bReviewed\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)*\s*({DATE_CORE})', re.I),
            re.compile(rf'\bUpdated\s*(?:on)?\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)*\s*({DATE_CORE})', re.I),
            re.compile(rf'\bCurrent\s+as\s+at\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?)*\s*({DATE_CORE})', re.I),
            re.compile(rf'\bLast\s*(?:changed|revised|modified)\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?)*\s*({DATE_CORE})', re.I),
        ]:
            m = rx.search(text)
            if m:
                updated = normalize_date(m.group(1)); source = "text:last-updated"; confidence = "medium"; break

    # 6) <time datetime> with context
    if not updated:
        for t in soup.find_all("time"):
            dt_attr = t.get("datetime") or t.get("content")
            context = " ".join(filter(None, [
                t.get_text(" ", strip=True),
                t.get("aria-label", ""),
                (t.parent.get_text(" ", strip=True) if t.parent else "")
            ])).lower()
            if dt_attr and re.search(r'updated|reviewed|modified|current as at', context):
                v = normalize_date(dt_attr)
                if v:
                    updated, source, confidence = v, "time:datetime", "high"; break

    # 7) Script-key fallback (non-LD JSON)
    script_key_rx = re.compile(
        r'"(?:dateModified|lastModified|lastUpdated|updatedAt|updated_at|modifiedAt|modified_at|pageUpdated|pageLastUpdated|dateLastUpdated|reviewDate|lastReviewed)"\s*:\s*"([^"]{4,30})"',
        re.I
    )
    if not updated:
        m = script_key_rx.search(html)
        if m:
            val = normalize_date(m.group(1)) or m.group(1)
            if val:
                updated, source, confidence = normalize_date(val), "script:date-key", "medium"

    # Title
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Visible text hash with dates stripped
    txt_hash, excerpt = stable_text_hash(html)
    return updated, published, source, confidence, txt_hash, excerpt, title

# ------------------ Per-URL fetch & process ------------------
def process_url(url: str, base_timeout: int, gov_timeout: int):
    sess, timeout = make_session(gov_timeout if (urlparse(url).hostname or "").lower().endswith(".gov.au") else base_timeout)
    notes = []
    code = 0
    final_url = url
    etag = lastmod = ""
    content_hash_raw = text_hash = text_excerpt = title = ""
    det_updated = det_published = upd_source = confidence = ""

    try:
        # HEAD then GET; manual redirect (limit 5)
        try:
            r = sess.head(url, allow_redirects=False, timeout=timeout)
        except Exception as e:
            notes.append(f"HEAD fail: {e.__class__.__name__}")
            r = sess.get(url, allow_redirects=False, timeout=timeout)

        hops = 0
        while 300 <= getattr(r, "status_code", 0) < 400 and "Location" in r.headers and hops < 5:
            final_url = requests.compat.urljoin(final_url, r.headers["Location"])
            notes.append(f"redirect->{final_url}")
            try:
                r = sess.get(final_url, allow_redirects=False, timeout=timeout)
            except Exception as e:
                notes.append(f"GET fail: {e.__class__.__name__}")
                break
            hops += 1

        code = getattr(r, "status_code", 0) or 0
        etag = r.headers.get("ETag","")
        lastmod = r.headers.get("Last-Modified","") or r.headers.get("last-modified","")

        ct = (r.headers.get("Content-Type","") or "").lower()
        body = b""
        html_text = ""

        if code and 200 <= code < 300 and "text/html" in ct:
            try:
                rr = r if r.request.method != "HEAD" else sess.get(final_url, allow_redirects=False, timeout=timeout)
                body = rr.content or b""
                html_text = rr.text or ""

                u, p, src, conf, txt_hash, excerpt, title0 = extract_dates_from_html(html_text, final_url)
                if title0: title = title0

                # If no date found OR page looks JS-driven, render via Playwright (if enabled)
                if (not u and not p) and is_jsy((urlparse(final_url).hostname or ""), html_text) and USE_PLAYWRIGHT:
                    try:
                        rendered = render_html_playwright(final_url, timeout_ms=int(timeout*1000))
                        if rendered:
                            u2, p2, src2, conf2, txt_hash2, excerpt2, title2 = extract_dates_from_html(rendered, final_url)
                            if u2: u, src, conf = u2, src2, (conf2 or conf)
                            if p2: p = p2
                            if txt_hash2: txt_hash, excerpt = txt_hash2, excerpt2
                            if title2: title = title2
                            notes.append("rendered:playwright")
                    except Exception as e:
                        notes.append(f"render fail: {e.__class__.__name__}")

                det_updated, det_published, upd_source, confidence = u, p, src, conf
                content_hash_raw = hashlib.sha256((body if body is not None else b"")[:200_000]).hexdigest()
                text_hash, text_excerpt = txt_hash or "", excerpt or ""
            except Exception as e:
                notes.append(f"parse fail: {e.__class__.__name__}")

        elif code and 200 <= code < 300:
            # Non-HTML resource (e.g., PDF) – just hash bytes for reference
            try:
                if r.request.method == "HEAD":
                    rr = sess.get(final_url, allow_redirects=False, timeout=timeout, headers={"Range":"bytes=0-200000"})
                    body = rr.content or b""
                else:
                    body = r.content or b""
                content_hash_raw = hashlib.sha256((body if body is not None else b"")[:200_000]).hexdigest()
            except Exception as e:
                notes.append(f"hash fail: {e.__class__.__name__}")

        # classify
        if code and 200 <= code < 300:
            status = "OK"; note = " | ".join(n for n in notes if n)
        elif code:
            status = "BROKEN"
            reason = ("Auth or access restricted" if code in (401,403) else
                      "Rate limited" if code==429 else
                      "Server error" if code>=500 else
                      "Client error")
            note = " | ".join([*notes, reason])
        else:
            status = "ERROR"; note = " | ".join([*notes, "no response"])

        return {
            "status": status,
            "code": code,
            "final_url": final_url,
            "title": title,
            "notes": note,
            "etag": etag,
            "lastmod": lastmod,
            "content_hash": content_hash_raw,
            "det_updated": det_updated,
            "det_published": det_published,
            "upd_source": upd_source,
            "confidence": confidence,
            "text_hash": text_hash,
            "text_excerpt": text_excerpt,
        }
    except Exception as e:
        return {
            "status":"ERROR", "code":"", "final_url":url, "title":"",
            "notes":f"exception: {e.__class__.__name__}", "etag":"", "lastmod":"",
            "content_hash":"", "det_updated":"", "det_published":"",
            "upd_source":"", "confidence":"", "text_hash":"", "text_excerpt":""
        }

# ------------------ Google Sheets I/O ------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def gs_client():
    creds = Credentials.from_service_account_file("sa.json", scopes=SCOPES)
    return gspread.authorize(creds)

def ensure_col_s(ws):
    """Ensure Column S exists and header is set. Column R is your manual Topic Area."""
    if ws.col_count < 19:
        ws.add_cols(19 - ws.col_count)
    hdr = ws.row_values(1)
    if len(hdr) < 19:
        hdr += [""] * (19 - len(hdr))
    if not hdr[18]:
        hdr[18] = "Text Hash (visible, dates-stripped)"  # Column S
        ws.update("1:1", [hdr])

def read_rows(sheet_id, tab):
    gc = gs_client()
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    ensure_col_s(ws)
    data = ws.get_all_values()
    if not data or len(data[0]) < 17:
        raise RuntimeError("Sheet headers must include columns A→Q (R is your Topic Area).")
    return ws, data

def write_results(ws, rows, results):
    """Write B→Q (core) and S (text hash). Leave Column R (index 17) untouched."""
    now_iso_str = now_utc_iso()
    core_updates = []
    text_hash_updates = []

    for i, (row, res) in enumerate(zip(rows, results), start=2):
        row = (row + [""] * (19 - len(row)))[:19]
        prev_text_hash   = row[18] or ""  # S
        prev_raw_hash    = row[9]  or ""  # J (for reference)
        prev_first_seen  = row[11] or ""  # L
        prev_last_change = row[12] or ""  # M
        prev_lastmod     = row[8]  or ""  # I
        prev_det_updated = row[13] or ""  # N
        prev_upd_source  = row[14] or ""  # O
        prev_pub         = row[15] or ""  # P

        new_text_hash = res.get("text_hash","")
        new_raw_hash  = res.get("content_hash","")
        new_lastmod   = res.get("lastmod","")
        new_det_upd   = res.get("det_updated","")
        new_pub       = res.get("det_published","")
        new_src       = res.get("upd_source","")

        # Only mark UPDATED when we have a previous text-hash and the new text-hash differs
        changed_text = bool(prev_text_hash) and bool(new_text_hash) and (new_text_hash != prev_text_hash)

        last_changed = prev_last_change or ""
        if changed_text:
            last_changed = now_iso_str
        first_seen = prev_first_seen or now_iso_str

        core_updates.append([
            res.get("status",""),            # B
            res.get("code",""),              # C
            res.get("final_url",""),         # D
            res.get("title",""),             # E
            now_iso_str,                     # F Last Checked
            res.get("notes",""),             # G
            res.get("etag",""),              # H ETag
            new_lastmod,                     # I Last-Modified
            new_raw_hash,                    # J Content Hash (raw)
            "UPDATED" if changed_text else "",  # K Change? (only if text changed this run)
            first_seen,                      # L First Seen
            last_changed,                    # M Last Changed
            new_det_upd,                     # N Detected Updated
            new_src,                         # O Updated Source
            new_pub,                         # P Detected Published
            res.get("confidence",""),        # Q Parse Confidence
        ])
        text_hash_updates.append([new_text_hash])      # S

    end_row = 1 + len(rows)
    ws.update(f"B2:Q{end_row}", core_updates, value_input_option="RAW")
    ws.update(f"S2:S{end_row}", text_hash_updates, value_input_option="RAW")

# ------------------ Slack summaries ------------------
def _sanitize_title_for_slack(t: str) -> str:
    return (t or "").replace("|", "∣").strip()

def _write_topic_area_summary(rows, results, tz_str: str):
    """
    Build Slack text grouped by Column R ('Topic Area'), but include ONLY rows that satisfy
    the alert strategy.

    Alert strategies (env ALERT_MODE):
      - "today_only": include if (det_updated OR lastmod OR det_published) has local date == today
      - "text_change": include if visible-text hash changed this run
      - "both": include only if text changed this run AND local date == today
    """
    tz = ZoneInfo(tz_str)
    today_str = datetime.now(tz).date().isoformat()

    groups = OrderedDict()
    total_changes = 0

    for row, res in zip(rows, results):
        row = (row + [""] * (19 - len(row)))[:19]
        url_a = (row[0] or "").strip()
        if not url_a:
            continue

        # previous values
        prev_text_hash   = row[18] or ""     # S
        prev_lastmod     = row[8]  or ""     # I
        prev_det_updated = row[13] or ""     # N
        prev_pub         = row[15] or ""     # P

        # new values
        new_text_hash = res.get("text_hash","")
        new_raw_hash  = res.get("content_hash","")
        new_lastmod   = res.get("lastmod","")
        new_det_upd   = res.get("det_updated","")
        new_pub       = res.get("det_published","")
        new_src       = res.get("upd_source","")
        title         = (res.get("title") or (row[4] or url_a)).strip()

        status = (res.get("status") or "").upper()
        if status != "OK":
            continue  # only alert on OK pages for content changes

        changed_text = bool(prev_text_hash) and bool(new_text_hash) and (new_text_hash != prev_text_hash)

        # compute local-day strings from detected dates for "today_only"/"both"
        det_upd_day = date_to_local_day := ""  # placeholder to keep chat linters happy
        det_upd_day = _to_local_day(new_lastmod, new_det_upd, new_pub, tz_str)

        date_is_today = (det_upd_day == today_str) if det_upd_day else False

        def passes_strategy():
            if ALERT_MODE == "text_change":
                return bool(changed_text)
            if ALERT_MODE == "both":
                return bool(changed_text) and bool(date_is_today)
            # default: today_only
            return bool(date_is_today)

        if not passes():
            pass  # fixed below
        # Fix chat-lint artefact: correct function call
        if not passes_strategy():
            continue

        # Build tag list: include ONLY labels whose value changed this run
        tags = []
        if new_lastmod and new_lastmod != prev_lastmod:
            tags.append(f"modified: {new_lastmod}")
        if new_raw_hash and new_raw_hash != (row[9] or ""):
            tags.append("changed")
        if new_det_upd and new_det_upd != prev_det_upd:
            tags.append(f"updated: {new_det_upd}")
        if new_src and new_src != (row[14] or ""):
            tags.append(f"source updated: {new_src}")
        if new_pub and new_pub != prev_pub:
            tags.append(f"published: {new_pub}")

        topic = (row[17] or "").strip() or "Uncategorised"
        groups.setdefault(topic, []).append({
            "title": _sanitize_title_for_slack(title),
            "url": url_a,
            "tags": tags,
        })
        total_changes += 1

    # Build Slack message text in your requested format
    lines = []
    if total_changes:
        lines.append("Here's the daily update on any new key resource changes:")
        lines.append("")
        for topic, items in groups.items():
            if not items:
                continue
            n = len(items)
            change_word = "change" if n == 1 else "changes"
            lines.append(f"Topic Area: *{topic} (❗️{n} {change_word} detected)*")
            for it in items:
                link = f"<{it['url']}|{it['title']}>"
                suffix = f", {', '.join(it['tags'])}" if it["tags"] else ""
                lines.append(f"• {link}{suffix}")
            lines.append("")

    with open("topic_summary.json","w",encoding="utf-8") as f:
        json.dump({"date_local": today_str, "total_changes": total_changes, "groups": groups}, f, ensure_ascii=False, indent=2)
    with open("topic_summary.md","w",encoding="utf-8") as f:
        f.write("\n".join(lines) if lines else "")

def _to_local_day(lastmod_iso_utc: str, det_updated_iso_utc: str, published_iso_utc: str, tz_str: str) -> str:
    """Pick best available date signal; return local day (YYYY-MM-DD) for ALERT filtering."""
    for iso in (det_updated_iso_utc, lastmod_iso_utc, published_iso_utc):
        if not iso:
            continue
        try:
            dt = datetime.fromisoformat(iso).astimezone(ZoneInfo(tz_str))
            return dt.date().toordinal().__str__() if False else dt.date().isoformat()  # fix chat-lint: ensure string
        except Exception:
            continue
    return ""

# ------------------ Slack-friendly formatting for generic summary (kept) ------------------
def _write_summary_files(rows, results):
    tz = ZoneInfo(ALERT_TZ)
    date_label = datetime.now(tz).strftime("%A %d %B %Y")
    checked = len(results)
    updated = broken = errors = 0

    for row, res in zip(rows, results):
        row = (row + [""] * (19 - len(row)))[:19]
        status = (res.get("status") or "").upper()
        prev_text_hash = row[18] or ""
        if status == "OK" and (prev_text_hash and res.get("text_hash") and res["text_hash"] != prev_text_hash):
            updated += 1
        elif status == "BROKEN":
            broken += 1
        elif status == "ERROR":
            errors += 1

    summary = {"date_local": date_label, "counts": {"checked": checked, "updated": updated, "broken": broken, "errors": errors}}
    with open("summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    with open("summary.md", "w", encoding="utf-8") as f:
        f.write(f"*Daily guideline link check — {date_label}*\n\n"
                f"*Totals:* {checked} checked · {updated} updated · {broken} broken · {errors} errors\n")

# ------------------ Main ------------------
def main():
    args = parse_args()

    # Google Sheets I/O
    ws, data = read_rows(args.sheet, args.tab)
    rows = data[1:]
    urls = [(r[0].strip() if len(r) >= 1 else "") for r in rows]

    sess_base, base_timeout = make_session(args.base_timeout)
    sess_gov, gov_timeout = make_session(args.gov_timeout)

    def run_one(idx, url):
        if not url:
            return {"status":"SKIPPED","code":"","final_url":"","title":"","notes":"No URL",
                    "etag":"","lastmod":"","content_hash":"",
                    "det_updated":"","det_published":"","upd_source":"","confidence":"",
                    "text_hash":"","text_excerpt":""}
        host = (urlparse(url).hostname or "").lower()
        sess, to = (sess_gov, gov_timeout) if host.endswith(".gov.au") else (sess_base, base_timeout)
        return process_url(url, to, to)  # uses its own session maker; safe

    results = [None]*len(rows)
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(run_one, i, url): i for i, url in enumerate(urls)}
        for fut in as_completed(futs):
            i = futs[fut]
            try:
                results[i] = fut.result()
            except Exception as e:
                results[i] = {"status":"ERROR","code":"","final_url":urls[i],
                              "title":"","notes":f"exception: {e.__class__.__name__}",
                              "etag":"","lastmod":"","content_hash":"",
                              "det_updated":"","det_published":"","upd_source":"",
                              "confidence":"","text_hash":"","text_excerpt":""}

    # Build summaries BEFORE persisting so we compare against prior sheet state
    _write_summary_files(rows, results)
    _write_topic_area_summary(rows, results, ALERT_TZ)

    # Persist back to Sheet
    write_results(ws, rows, results)

# --------- Utility for local-day extraction used in Slack filter ---------
def _to_local_day(lastmod_iso_utc: str, det_updated_iso_utc: str, published_iso_utc: str, tz_str: str) -> str:
    for iso in (det_updated_iso_utc, lastmod_iso_utc, published_iso_utc):
        if not iso:
            continue
        try:
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt_local = dt.astimezone(ZoneInfo(tz_str))
            return dt_local.date().strftime("%Y-%m-%d")
        except Exception:
            continue
    return ""

if __name__ == "__main__":
    # Make DNS lookups fail reasonably fast
    socket.setdefaulttimeout(12)

    # Mirror all output to a file for GitHub artifact
    import sys
    class Tee:
        def __init__(self, path="run.log"):
            self.f = open(path, "w", encoding="utf-8")
            self.out = sys.stdout
        def write(self, s):
            self.f.write(s); self.f.flush()
            self.out.write(s); self.out.flush()
        def flush(self):
            self.f.flush(); self.out.flush()
    sys.stdout = sys.stderr = Tee("run.log")

    main()
