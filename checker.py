import os, re, json, hashlib, argparse, socket, contextlib
from datetime import datetime, timezone
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
    return ap.deepcopy().parse_args() if hasattr(ap, "deepcopy") else ap.parse_args()

USE_PLAYWRIGHT = os.getenv("USE_PLAYWRIGHT", "0") == "1"
RENDER_DOMAINS = {d.strip().lower() for d in os.getenv("RENDER_DOMAINS", "").split(",") if d.strip()}

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
        "User-Agent": "GH-Actions-LinkChecker/1.2 (+https://github.com/)",
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.8",
        "Cache-Control": "no-cache",
    })
    sess.request_timeout = timeout_s  # doc-only; we pass timeout per request
    return sess

# ------------------ Date & text helpers ------------------
MONTHS = {m:i for i,m in enumerate(
    ["january","february","march","april","may","june","july",
     "august","september","october","november","december"])}
MONTHS.update({"jan":0,"feb":1,"mar":2,"apr":3,"may2":4,"jun":5,"jul":6,"aug":7,"sep":8,"sept":8,"oct":9,"nov":10,"dec":11})

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def is_gov_au(host: str) -> bool:
    return host.endswith(".gov.au")

def limit_bytes(b: bytes, cap: int = 200_000) -> bytes:
    return b if b is not None and len(b) <= cap else (b[:cap] if b else b"")

def to_iso(dt: datetime) -> str:
    try:
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ""

def _parse_dayfirst_numeric(s: str) -> str:
    """Handle dd/mm/yyyy and dd.mm.yyyy and dd-mm-yyyy."""
    s = s.strip()
    for rx in (r"(\d{1,2})/(\d{1,2})/(\d{4})",
               r"(\d{1,2})\.(\d{1,2})\.(\d{4})",
               r"(\d{1,2})-(\d{1,2})-(\d{4})"):
        m = re.search(rx, s)
        if m:
            d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            try:
                dt = datetime(y, mth, d, tzinfo=timezone.utc)
                return to_iso(dt)
            except Exception:
                return ""
    return ""

def normalize_date(s: str) -> str:
    """Convert many human/ISO date spellings into ISO8601 UTC string."""
    if not s:
        return ""
    s = s.strip()

    # Strip leading weekday names like "Thursday" or "Thu,"
    s = re.sub(
        r'^(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?),?\s+',
        '',
        s,
        flags=re.I
    )

    # ISO-like (allow trailing Z)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return to_iso(dt)
    except Exception:
        pass

    # Common long/short formats
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%b %d, %Y", "%B %d, %Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return to_iso(dt)
        except Exception:
            pass

    # Month Year -> assume 1st of month
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{4})", s)
    if m:
        mon = m.group(1).lower()
        mon = "may2" if mon == "may" else mon
        if mon in MONTHS:
            dt = datetime(int(m.group(2)), MONTHS[mon] + 1, 1, tzinfo=timezone.utc)
            return to_iso(dt)

    # yyyy-mm-dd anywhere
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        return to_iso(dt)

    # dd/mm/yyyy or dd.mm.yyyy or dd-mm-yyyy
    val = _parse_dayfirst_numeric(s)
    if val:
        return val

    return ""

# Patterns for date extraction and noise removal
WEEKDAY_OPT = r'(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\s+'
DATE_CORE   = r'(?:\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})'

META_UPDATED = [
    (re.compile(r'<meta[^>]+property=["\']article:modified_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:article:modified_time"),
    (recompile := re.compile)(r'<meta[^>]+property=["\']og:updated_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:og:updated_time",
    (re.compile(r'<meta[^>]+itemprop=["\']dateModified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:itemprop:dateModified"),
    (re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', r.I), "meta:last-modified"),
    (re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', r.I), "meta:modified"),
    (re.compile(r'<meta[^>]+name=["\']dc\.date\.modified["\'][^>]+content=["\']([^"\']+)["\']', r.I), "meta:dc.date.modified"),
    (re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', r.I), "meta:DCTERMS.modified"),
    (re.compile(r'<meta[^>]+name=["\']DC\.Date\.Modified["\'][^>]+content=["\']([^"\']+)["\']', r.I), "meta:DC.Date.Modified"),
]

META_PUBLISHED = [
    (re.compile(r'<meta[^>]+(itemprop|name|property)=["\']datePublished["\'][^>]+content=["\']([^"\']+)["\']', re.I), 2),
    (re.compile(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), 1),
    (re.compile(r'<meta[^>]+name=["\']dc\.date["\'][^>]+content=["\']([^"\']+)["\']', re.I), 1),
]

DATE_WORDS   = r'(?:Last\s*updated|Page\s*last\s*updated|Last\s*reviewed|Reviewed|Updated(?:\s*on)?|Current\s+as\s+at|Last\s*(?:changed|revised|modified))'
DATE_TOKEN   = r'(?:\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})(?:\s+\d{2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM))?)?'
TIMESTAMP_NOISE = re.compile(rf'{DATE_WORDS}\s*[:\-–—]?\s*{DATE_TOKEN}|{DATE_TOKEN}', re.I)

def visible_text_hash(html: str):
    """Return (stable_text_hash, excerpt) from visible text with date-ish substrings removed."""
    soup = BeautifulSoup(html or "", "lxml")
    for el in soup(["script", "style", "noscript"]):
        el.extract()
    text = (soup.get_text(" ", strip=True) or "")
    norm = re.sub(r"\s+", " ", text).strip().lower()
    # strip volatile date markers
    stable = re.sub(TIMESTAMP_NOISE, "", norm)
    excerpt = text[:240]
    return sha256_hex(stable.encode("utf-8")), excerpt

# ------------------ Date extraction (static & rendered) ------------------
def extract_dates_from_html(html: str, url: str):
    updated = published = source = confidence = ""

    soup = BeautifulSoup(html or "", "lxml")
    for el in soup(["script","style","noscript"]): 
        el.extract()
    text = re.sub(r"\s+", " ", (soup.get_text(" ", strip=True) or "").replace("\xa0"," ")).strip()

    # 1) Meta tags
    for rx, src in META_KEYED := [
        (re.compile(r'<meta[^>]+property=["\']article:modified_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:article:modified_time"),
        (re.compile(r'<meta[^>]+property=["\']og:updated_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:og:updated_time"),
        (re.compile(r'<meta[^>]+itemprop=["\']dateModified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:itemprop:dateModified"),
        (re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified"),
        (re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified"),
        (re.compile(r'<meta[^>]+name=["\']dc\.date\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:dc.date.modified"),
        (re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DCTERMS.modified"),
        (re.compile(r'<meta[^>]+name=["\']DC\.Date\.Modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DC.Date.Modified"),
    ]:
        m = rx.search(html)
        if m:
            updated = normalize_date(m.group(1)); source = src; confidence = "high"; break

    for rx, gi in META_PUBLISHED:
        if not published:
            m = rx.search(html)
            if m:
                published = normalize_date(m.group(gi))

    # 2) JSON-LD
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

    # 3) Site-specific hints
    host = (urlparse(url).hostname or "").lower()
    def find_html(rx, src, conf):
        nonlocal updated, source, confidence
        if not updated:
            m = rx.search(html)
            if m:
                val = m.group(2) if (m.lastindex and m.lastindex >= 2) else m.group(1)
                val = normalize_date(val)
                if val:
                    updated = val; source = src; confidence = conf

    if "health.nsw.gov.au" in host:
        weekday_opt = r'(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?)\s+'
        find_html(re.compile(rf'Current\s+as\s+at[^<:]*[:>]\s*(?:{weekday_opt})?({DATE_CORE})', re.I),
                  "text:NSW:current-as-at", "high")
        find_html(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I),
                  "meta:modified", "high")
        find_html(re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I),
                  "meta:DCTERMS.modified", "high")

    if "racgp.org.au" in host:
        find_html(re.compile(r'(Last\s*updated|Reviewed)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})', re.I),
                  "text:RACGP", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I),
                  "meta:last-modified", "high")

    # 4) Plain-text fallbacks
    for rx in [
        re.compile(rf'\bLast\s*updated\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', re.I),
        re.compile(rf'\bPage\s*last\s*updated\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', r.I),
        re.compile(rf'\bLast\s*reviewed\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', r.I),
        re.compile(rf'\bReviewed\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', r.I),
        re.compile(rf'\bUpdated\s*(?:on)?\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', r.I),
        re.compile(rf'\bCurrent\s+as\s+at\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', r.I),
        re.compile(rf'\bLast\s*(?:changed|revised|modified)\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', r.I),
    ]:
        if updated:
            break
        m = rx.search(text)
        if m:
            val = normalize_date(m.group(1))
            if val:
                updated = val; source = "text:last-updated"; confidence = "medium"; break

    # 5) <time datetime> with context
    if not updated:
        for t in soup.find_all("time"):
            dt_attr = t.get("datetime") or t.get("content")
            context = " ".join(filter(None, [
                t.get_text(" ", strip=True),
                t.get("aria-label", ""),
                (t.parent.get_text(" ", strip=True) if t.parent else "")
            ])).lower()
            if dt_attr and re.search(r'updated|reviewed|modified|current as at', context):
                val = normalize_date(dt_attr)
                if val:
                    updated = val; source = "time:datetime"; confidence = "high"; break

    # 6) Script-key fallback
    script_key_rx = re.compile(
        r'"(?:dateModified|lastModified|lastUpdated|updatedAt|updated_at|modifiedAt|modified_at|pageUpdated|pageLastUpdated|dateLastUpdated|reviewDate|lastReviewed)"\s*:\s*"([^"]{4,30})"',
        re.I
    )
    if not updated:
        m = script_key_rx.search(html)
        if m:
            val = normalize_date(m.group(1)) or m.group(1)
            if val:
                updated = normalize_date(val); source = "script:date-key"; confidence = "medium"

    # Title
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Visible text hash (stable)
    txt_hash, excerpt = visible_text_hash(html)
    return updated, published, source, confidence, txt_hash, excerpt, title

# ------------------ Optional JS rendering ------------------
def should_render(host: str, html_text: str) -> bool:
    if not USE_PLAYWRIGHT:
        return False
    host = (host or "").lower()
    if RENDER_DOMAINS and not any(host.endswith(d) or d in host for d in RENDER_DOMAINS):
        return False
    # Heuristics for SPA/scripted content
    if re.search(r'\bLoading\.\.\.|enable\s+scripts|this\s+site\s+requires\s+javascript', html_text or "", flags=re.I):
        return True
    return True  # allow rendering when enabled to be safe

def render_html_playwright(url: str, timeout_ms: int = 25000) -> str:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        try:
            page = browser.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(url, wait_until="networkidle")
            with contextlib.suppress(Exception):
                page.wait_for_load_state("networkidle")
            html = page.content()
            return html or ""
        finally:
            with contextlib.suppress(Exception):
                page.close()
            with contextlib.suppress(Exception):
                browser.close()

# ------------------ Per-URL processing ------------------
def process_url(url: str, base_timeout: int, gov_timeout: int):
    host = (urlparse(url).hostname or "").lower()
    timeout = gov_timeout if is_gov_au(host) else base_timeout

    sess = make_session(timeout_s=timeout)
    notes = []
    code = ""
    final_url = url
    etag = lastmod = ""
    content_hash_raw = text_hash = text_excerpt = title = ""
    detected_updated = detected_published = updated_source = confidence = ""

    try:
        # Prefer HEAD then GET
        try:
            r = sess.head(url, allow_redirects=False, timeout=timeout)
        except Exception as e:
            notes.append(f"HEAD fail: {e.__class__.__name__}")
            r = sess.get(url, allow_redirects=False, timeout=timeout)

        # Follow redirects (cap 5)
        hops = 0
        while 300 <= r.status_code < 400 and "Location" in r.headers and hops < 5:
            final_url = requests.compat.urljoin(final_url, r.headers["Location"])
            notes.append(f"redirect->{final_url}")
            try:
                r = sess.get(final_url, allow_redirects=False, timeout=timeout)
            except Exception as e:
                notes.append(f"GET fail: {e.__class__.__name__}")
                break
            hops += 1

        code = r.status_code
        etag = r.headers.get("ETag","")
        lastmod = r.headers.get("Last-Modified","") or r.headers.get("last-modified","")

        ct = (r.headers.get("Content-Type","") or "").toLowerCase() if False else (r.headers.get("Content-Type","") or "").lower()
        body = b""
        html_text = ""
        if code and 200 <= code < 300 and "text/html" in ct:
            try:
                rr = r if r.request.method != "HEAD" else sess.get(final_url, allow_redirects=False, timeout=timeout)
                body = rr.content or b""
                html_text = rr.text or ""
                # Extract from static HTML
                u,p,src,conf,txt_hash,excerpt,title0 = extract_dates_from_html(html_text, final_url)
                if title0: title = title0

                # If no dates found OR likely JS-driven, render
                if (not u and not p) and should_render(host, html_text):
                    try:
                        rendered = render_html_playwright(final_url, timeout_ms=(timeout*1000))
                        if rendered:
                            u2,p2,src2,conf2,txt_hash2,excerpt2,title2 = extract_dates_from_html(rendered, final_url)
                            if u2: u, src, conf = u2, src2, conf2 or conf
                            if p2: p = p2
                            if title2: title = title2
                            if txt_hash2: 
                                txt_hash, excerpt = txt_hash2, excerpt2
                            notes.append("rendered:playwright")
                    except Exception as e:
                        notes.append(f"render fail: {e.__class__.__name__}")

                detected_updated, detected_published, updated_source, confidence = u,p,src,conf
                content_bytes = body if body is not None else b""
                content_hash_raw = sha256_hex(limit_bytes(content_bytes, 200_000))
                text_hash, text_excerpt = txt_hash or "", excerpt or ""
            except Exception as e:
                notes.append(f"parse fail: {e.__class__.__name__}")

        elif code and 200 <= code < 300:
            # Non-HTML: just hash bytes for reference (no text hash)
            try:
                if r.request.method == "HEAD":
                    rr = sess.get(final_url, allow_redirects=False, timeout=timeout, headers={"Range":"bytes=0-200000"})
                    body = rr.content or b""
                else:
                    body = r.content or b""
                content_hash_raw = sha256_hex(limit_bytes(body, 200_000))
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
            note = " | ".join([*notes, f"{reason}"])
        else:
            status = "ERROR"; note = " | ".join([*notes, "no response"])

        return dict(
            status=status, code=code, final_url=final_url, title=title, notes=note,
            etag=etag, lastmod=lastmod, content_hash=content_hash_raw,
            det_updated=u, det_published=p, upd_source=src, confidence=conf,
            text_hash=text_hash, text_excerpt=text_excerpt
        )
    except Exception as e:
        return dict(
            status="ERROR", code="", final_url=url, title="",
            notes=f"exception: {e.__class__.__name__}", etag="", lastmod="",
            content_hash="", det_updated="", det_published="",
            upd_source="", confidence="", text_hash="", text_excerpt=""
        )

# ------------------ Google Sheets I/O ------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def gs_client():
    creds = Credentials.from_service_account_file("sa.json", scopes=SCOPES)
    return gspread.authorize(creds)

def ensure_col_s_header(ws):
    # Ensure there are at least 19 columns (A..S) and S1 header text
    if ws.col_count < 19:
        ws.add_cols(19 - ws.col_count)
    header = ws.row_values(1)
    if len(header) < 19:
        header += [""] * (19 - len(header))
    if not header[18]:
        header[18] = "Text Hash (visible, dates-stripped)"  # Column S
        ws.update("1:1", [header])

def read_rows(sheet_id, tab):
    gc = gs_client()
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    # Make sure Column S exists before reading all values (so we can safely write later)
    ensure_col_s_header(ws)
    data = ws.get_all_values()
    if not data or len(data[0]) < 17:
        raise RuntimeError("Sheet headers must include columns A→Q (R is your Topic Area).")
    return ws, data

def write_results(ws, rows, results):
    """
    Writes core metrics to B→Q (leaves Column R untouched), and writes Text Hash to S.
    Marks Change? (K) as UPDATED only when the **visible text hash (S)** changes.
    """
    now = now_iso()
    updates_core = []
    updates_text_hash = []

    for i, (row, res) in enumerate(zip(rows, results), start=2):
        row = (row + [""] * (19 - len(row)))[:19]

        prev_etag        = row[7]  or ""   # H
        prev_lastmod     = row[8]  or ""   # I
        prev_hash_raw    = row[9]  or ""   # J
        prev_first_seen  = row[11] or ""   # L
        prev_last_change = row[12] or ""   # M
        prev_det_updated = row[13] or ""   # N
        prev_upd_source  = row[14] or ""   # O
        prev_published   = row[15] or ""   # P
        prev_text_hash   = row[18] or row[9] or ""  # S or fallback to raw hash

        new_text_hash = res.get("text_hash","")
        new_raw_hash  = res.get("content_hash","")
        new_lastmod   = res.get("lastmod","")
        new_etag      = res.get("etag","")
        new_det_upd   = res.get("det_updated","")
        new_pub       = res.get("det_published","")
        new_src       = res.get("upd_source","")

        changed_text = bool(new_text_hash) and (new_text_hash != prev_text_hash)
        first_seen   = prev_first_seen or now
        last_changed = now if changed_text else (prev_last_change or "")

        updates_core.append([
            res.get("status",""),        # B Status
            res.get("code",""),          # C HTTP Code
            res.get("final_url",""),     # D Final URL
            res.get("title",""),         # E Title
            now,                          # F Last Checked (UTC ISO)
            res.get("notes",""),          # G Notes
            new_etag,                     # H ETag (header)
            new_lastmod,                  # I Last-Modified (header)
            new_raw_hash,                 # J Content Hash (raw HTML bytes)
            "UPDATED" if changed_text else "",  # K Change? (text-hash based)
            first_seen,                   # L First Seen
            last_changed,                 # M Last Changed (on text change)
            new_det_upd,                  # N Detected Updated
            new_src,                      # O Updated Source
            new_pub,                      # P Detected Published
            res.get("confidence",""),     # Q Parse Confidence
        ])

        updates_text_hash.append([new_text_hash])  # S

    end_row = 1 + len(rows)
    ws.update(f"B2:Q{end_row}", updates_core, value_input_option="RAW")
    # Ensure S exists & header is present (Column R is user's Topic Area at index 16; S is 19th col index 18)
    if ws.col_count < 19:
        ws.add_cols(19 - ws.col_count)
    header = ws.row_values(1)
    if len(header) < 19:
        header += [""] * (19 - len(header))
    if not header[18]:
        header[18] = "Text Hash (visible, dates-stripped)"
        ws.update("1:1", [header])
    ws.update(f"S2:S{end_row}", updates_text_hash, value_input_option="RAW")

# ------------------ Slack summaries ------------------
def _sanitize_title_for_slack(t: str) -> str:
    return (t or "").replace("|", "∣").strip()

def _write_summary_files(rows, results):
    """Generic rollup (still produced for reference; not used for alerts)."""
    tz = ZoneInfo("Australia/Melbourne")
    date_label = datetime.now(tz).format("%A %d %B %Y") if hasattr(datetime.now(tz), "format") else datetime.now(tz).strftime("%A %d %B %Y")

    checked = len(results)
    updated = broken = errors = 0
    lines = [f"*Daily guideline link check — {date_label}*", ""]
    for row, res in zip(rows, results):
        status = (res.get("status") or "").upper()
        if status == "OK" and (res.get("text_hash") and (res.get("text_hash") != ( (row + ['']* (19-len(row))))[18] )):
            updated += 1
        elif status == "BROKEN":
            broken += 1
        elif status == "ERROR":
            errors += 1
    lines.append(f"*Totals:* {checked} checked · {updated} updated · {broken} broken · {errors} errors")
    lines.append("")
    with open("summary.json","w",encoding="utf-8") as f:
        json.dump({"date_local": date_label, "counts":{"checked":checked,"updated":updated,"broken":broken,"errors":errors}}, f, ensure_ascii=False, indent=2)
    with open("summary.md","w",encoding="utf-8") as f:
        f.write("\n".join(lines))

def _write_topic_area_summary(rows, results, tz_str="Australia/Melbourne"):
    """
    Build Slack text grouped by Column R ('Topic Area'), showing ONLY items that
    had a NEW significant change this run (visible text hash changed). For each
    changed item, list any date labels that ALSO changed this run in this order:
    modified, changed, updated, source updated, published.
    """
    tz = ZoneInfo(tz_str)
    date_label = datetime.now(tz).strftime("%A %d %B %Y")

    groups = OrderedDict()   # {topic_area: [ {title,url,tags[]} ]}
    total_changes = 0

    for row, res in zip(rows, results):
        row = (row + [""] * (19 - len(row)))[:19]
        url_colA = (row[0] or "").strip()
        if not url_colA:
            continue

        prev_lastmod     = row[8]  or ""   # I
        prev_hash_raw    = row[9]  or ""   # J
        prev_first_seen  = row[11] or ""   # L (not used here)
        prev_last_change = row[12] or ""   # M (not used here)
        prev_det_updated = row[13] or ""   # N
        prev_upd_source  = row[14] or ""   # O
        prev_published   = row[15] or ""   # P
        prev_text_hash   = row[18] or row[9] or ""

        new_text_hash = res.get("text_hash","")
        new_raw_hash  = res.get("content_hash","")
        new_lastmod   = res.get("lastmod","")
        new_det_upd   = res.get("det_updated","")
        new_pub       = res.get("det_published","")
        new_src       = res.get("upd_source","")
        title         = (res.get("title") or row[4] or url_colA).strip()

        changed_text = bool(new_text_hash) and (new_text_hash != prev_text_hash)
        if not changed_text:
            continue

        tags = []
        if new_lastmod and new_lastmod != prev_lastmod:
            tags.append(f"modified: {new_lastmod}")
        if new_raw_hash and new_raw_hash != prev_hash_raw:
            tags.append("changed")
        if new_det_upd and new_det_upd != prev_det_updated:
            tags.append(f"updated: {new_det_upd}")
        if new_src and new_src != prev_upd_source:
            tags.append(f"source updated: {new_src}")
        if new_pub and new_pub != prev_published:
            tags.append(f"published: {new_pub}")

        topic = (row[17] or "").strip() or "Uncategorised"  # Column R
        groups.setdefault(topic, []).append({
            "title": _sanitize_title_for_slack(title),
            "url": url_colA,
            "tags": tags,
        })
        total_changes += 1

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
        json.dump({"date_local": date_label, "total_changes": total_changes, "groups": groups}, f, ensure_ascii=False, indent=2)
    with open("topic_summary.md","w",encoding="utf-8") as f:
        f.write("\n".join(lines) if lines else "")

# ------------------ Core check logic ------------------
def fetch_and_process(url: str, base_timeout: int, gov_timeout: int):
    host = (urlparse(url).hostname or "").lower()
    timeout = gov_timeout if is_gov_au(host) else base_timeout

    sess = make_session(timeout_s=timeout)
    notes = []
    code = 0
    final_url = url
    etag = lastmod = ""
    content_hash_raw = text_hash = text_excerpt = title = ""
    u = p = src = conf = ""

    try:
        try:
            r = sess.head(url, allow_redirects=False, timeout=timeout)
        except Exception as e:
            notes.append(f"HEAD fail: {e.__class__.__name__}")
            r = sess.get(url, allow_redirects=False, timeout=timeout)

        hops = 0
        while 300 <= r.status_code < 400 and "Location" in r.headers and hops < 5:
            final_url = requests.compat.urljoin(final_url, r.headers["Location"])
            notes.append(f"redirect->{final_url}")
            try:
                r = sess.get(final_url, allow_redirects=False, timeout=timeout)
            except Exception as e:
                notes.append(f"GET fail: {e.__class__.__name__}")
                break
            hops += 1

        code = r.status_code
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

                if (not u and not p) and should_render(host, html_text):
                    try:
                        rendered = render_html_playwright(final_url, timeout_ms=(timeout*1000))
                        if rendered:
                            u2, p2, src2, conf2, txt_hash2, excerpt2, title2 = extract_dates_from_html(rendered, final_url)
                            if u2: u, src, conf = u2, src2, conf2 or conf
                            if p2: p = p2
                            if title2: title = title2
                            if txt_hash2:
                                txt_hash, excerpt = txt_hash2, excerpt2
                            notes.append("rendered:playwright")
                    except Exception as e:
                        notes.append(f"render fail: {e.__class__.__name__}")

                content_hash_raw = sha256_hex(limit_bytes(body, 200_000))
                text_hash, text_excerpt = txt_hash or "", excerpt or ""
            except Exception as e:
                notes.append(f"parse fail: {e.__class__.__name__}")

        elif code and 200 <= code < 300:
            try:
                if r.request.method == "HEAD":
                    rr = sess.get(final_url, allow_redirects=False, timeout=timeout, headers={"Range":"bytes=0-200000"})
                    body = rr.content or b""
                else:
                    body = r.content or b""
                content_hash_raw = sha256_hex(limit_bytes(body, 200_000))
            except Exception as e:
                notes.append(f"hash fail: {e.__class__.__name__}")

        status = "OK" if (code and 200 <= code < 300) else ("BROKEN" if code else "ERROR")
        if status == "BROKEN":
            reason = ("Auth or access restricted" if code in (401,403) else
                      "Rate limited" if code==429 else
                      "Server error" if code>=500 else
                      "Client error")
            note = " | ".join([*notes, reason])
        else:
            note = " | ".join(n for n in notes if n) if status == "OK" else " | ".join([*notes, "no response"])

        return dict(
            status=status, code=code, final_url=final_url, title=title, notes=note,
            etag=etag, lastmod=lastmod, content_hash=content_hash_raw,
            det_updated=u, det_published=p, upd_source=src, confidence=conf,
            text_hash=text_hash, text_excerpt=text_excerpt
        )
    except Exception as e:
        return dict(
            status="ERROR", code="", final_url=url, title="",
            notes=f"exception: {e.__class__.__name__}", etag="", lastmod="",
            content_hash="", det_updated="", det_published="",
            upd_source="", confidence="", text_hash="", text_excerpt=""
        )

# ------------------ Sheet I/O ------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def gs_client():
    creds = Credentials.from_service_account_file("sa.json", scopes=SCOPES)
    return gspread.authorize(creds)

def read_rows(sheet_id, tab):
    gc = gs_client()
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    # Ensure Column S exists and header label is present (R is your manual column)
    if ws.col_count < 19:
        ws.add_cols(19 - ws.col_count)
    header = ws.row_values(1)
    if len(header) < 19:
        header += [""] * (19 - len(header))
    if not header[18]:
        header[18] = "Text Hash (visible, dates-stripped)"  # column S
        ws.update("1:1", [header])
    data = ws.get_all_values()
    if not data or len(data[0]) < 17:
        raise RuntimeError("Sheet headers must include columns A→Q (R is your Topic Area).")
    return ws, data

def write_results(ws, rows, results):
    """Write B→Q (core) and S (text hash). Leave Column R untouched."""
    now = now_iso()
    updates_core = []
    updates_text_hash = []

    for i, (row, res) in enumerate(zip(rows, results), start=2):
        row = (row + [""] * (19 - len(row)))[:19]
        prev_text_hash   = row[18] or row[9] or ""  # S or fallback to raw J
        new_text_hash = res.get("text_hash","")
        changed_text = bool(new_text_hash) and (new_text_hash != prev_text_hash)

        last_changed = row[12] or ""
        if changed_text:
            last_changed = now
        first_seen = row[11] or now

        updates_core.append([
            res.get("status",""),        # B Status
            res.get("code",""),          # C HTTP Code
            res.get("final_url",""),     # D Final URL
            res.get("title",""),         # E Title
            now,                          # F Last Checked
            res.get("notes",""),          # G Notes
            res.get("etag",""),           # H ETag
            res.get("lastmod",""),        # I Last-Modified
            res.get("content_hash",""),   # J Raw Content Hash
            "UPDATED" if changed_text else "",  # K Change? (text-hash)
            first_seen,                   # L First Seen
            last_changed,                 # M Last Changed
            res.get("det_updated",""),    # N Detected Updated
            res.get("upd_source",""),     # O Updated Source
            res.get("det_published",""),  # P Detected Published
            res.get("confidence",""),     # Q Parse Confidence
        ])
        updates_text_hash.append([new_text_hash])

    end_row = 1 + len(rows)
    ws.update(f"B2:Q{end_row}", updates_core, value_input_option="RAW")
    ws.update(f"S2:S{end_row}", updates_text_hash, value_input_option="RAW")

# ------------------ Summaries ------------------
def _write_summary_files(rows, results):
    tz = ZoneInfo("Australia/Melbourne")
    date_label = datetime.now(tz).strftime("%A %d %B %Y")

    checked = len(results)
    updated = broken = errors = 0
    for row, res in zip(rows, results):
        row = (row + [""] * (19 - len(row)))[:19]
        status = (res.get("status") or "").upper()
        prev_text_hash = row[18] or row[9] or ""
        if status == "OK" and (res.get("text_hash") and res["text_hash"] != prev_text_hash):
            updated += 1
        elif status == "BROKEN":
            broken += 1
        elif status == "ERROR":
            errors += 1

    summary = {"date_local": date_label, "counts":{"checked":checked,"updated":updated,"broken":broken,"errors":errors}}
    with open("summary.json","w",encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    lines = [f"*Daily guideline link check — {date_label}*", "",
             f"*Totals:* {checked} checked · {updated} updated · {broken} broken · {errors} errors", ""]
    with open("summary.md","w",encoding="utf-8") as f:
        f.write("\n".join(lines))

def _write_topic_area_summary(rows, results, tz_str="Australia/Melbourne"):
    """Only include rows where visible-text hash changed this run. Group by Column R."""
    tz = ZoneInfo(tz_str)
    date_label = datetime.now(tz).strftime("%A %d %B %Y")

    groups = OrderedDict()
    total_changes = 0

    for row, res in zip(rows, results):
        row = (row + [""] * (19 - len(row)))[:19]
        url_colA = (row[0] or "").strip()
        if not url_colA:
            continue

        prev_lastmod     = row[8]  or ""   # I
        prev_hash_raw    = row[9]  or ""   # J
        prev_det_updated = row[13] or ""   # N
        prev_upd_source  = row[14] or ""   # O
        prev_published   = row[15] or ""   # P
        prev_text_hash   = row[18] or row[9] or ""

        new_text_hash = res.get("text_hash","")
        new_raw_hash  = res.get("content_hash","")
        new_lastmod   = res.get("lastmod","")
        new_det_upd   = res.get("det_updated","")
        new_pub       = res.get("det_published","")
        new_src       = res.get("upd_source","")
        title         = (res.get("title") or row[4] or url_colA).strip()

        changed_text = bool(new_text_hash) and (new_text_hash != prev_text_hash)
        if not changed_text:
            continue

        tags = []
        if new_lastmod and new_lastmod != prev_lastmod:
            tags.append(f"modified: {new_lastmod}")
        if new_raw_hash and new_raw_hash != prev_hash_raw:
            tags.append("changed")
        if new_det_upd and new_det_upd != prev_det_updated:
            tags.append(f"updated: {new_det_upd}")
        if new_src and new_src != prev_upd_source:
            tags.append(f"source updated: {new_src}")
        if new_pub and new_pub != prev_published:
            tags.append(f"published: {new_pub}")

        topic = (row[17] or "").strip() or "Uncategorised"  # Column R
        groups.setdefault(topic, []).append({
            "title": _sanitize_title_for_slack(title),
            "url": url_colA,
            "tags": tags,
        })
        total_changes += 1

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
        json.dump({"date_local": date_label, "total_changes": total_changes, "groups": groups}, f, ensure_ascii=False, indent=2)
    with open("topic_summary.md","w",encoding="utf-8") as f:
        f.write("\n".join(lines) if lines else "")

# ------------------ Main ------------------
def main():
    args = parse_args()

    ws, all_rows = read_rows(args.sheet, args.tab)
    # Build parallel arrays without dropping blank URL rows to keep row alignment
    rows = all_rows[1:]
    urls = [(r[0].strip() if len(r) >= 1 else "") for r in rows]

    base_timeout = args.base_timeout
    gov_timeout = args.gov_timeout

    results = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {}
        for idx, url in enumerate(urls):
            if not url:
                results[idx] = dict(
                    status="SKIPPED", code="", final_url="", title="", notes="No URL",
                    etag="", lastmod="", content_hash="",
                    det_updated="", det_published="",
                    upd_source="", confidence="",
                    text_hash="", text_excerpt=""
                )
            else:
                futs[ex.submit(fetch_and_process, url, base_timeout, gov_timeout)] = idx

        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                results[idx] = fut.result()
            except Exception as e:
                results[idx] = dict(
                    status="ERROR", code="", final_url=urls[idx], title="",
                    notes=f"exception: {e.__class__.__name__}",
                    etag="", lastmod="", content_hash="",
                    det_updated="", det_published="",
                    upd_source="", confidence="",
                    text_hash="", text_excerpt=""
                )

    # Build summaries BEFORE writing (so we compare against prior sheet state)
    _write_summary_files(rows, results)
    _write_topic_area_summary(rows, results)

    # Persist results back to the sheet (B→Q and S)
    write_results(ws, rows, results)

if __name__ == "__main__":
    # Make DNS lookups fail reasonably fast
    socket.setdefaulttimeout(12)

    # Stream logs to file + stdout
    import sys
    class Tee:
        def __init__(self, path):
            self.f = open(path, "w", encoding="utf-8")
            self.out = sys.stdout
        def write(self, b):
            self.f.write(b); self.f.flush()
            self.out.write(b); self.out.flush()
        def flush(self): 
            self.f.flush(); self.out.flush()
    sys.stdout = sys.stderr = Tee("run.log")

    main()
