import os, re, json, time, hashlib, argparse, socket, contextlib
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials

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
RENDER_DOMAINS = set([h.strip().lower() for h in os.getenv("RENDER_DOMAINS", "").split(",") if h.strip()])

# ------------------ HTTP session with retries ------------------
def make_session(timeout_s: int):
    sess = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.5,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=["HEAD","GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({
        "User-Agent": "GH-Actions-LinkChecker/1.1 (+https://github.com/)",
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.8",
        "Cache-Control": "no-cache",
    })
    sess.request_timeout = timeout_s
    return sess

# ------------------ Helpers ------------------
MONTHS = {m:i for i,m in enumerate(
    ["january","february","march","april","may","june","july",
     "august","september","october","november","december"]) }
MONTHS.update({"jan":0,"feb":1,"mar":2,"apr":3,"may2":4,"jun":5,"jul":6,"aug":7,"sep":8,"sept":8,"oct":9,"nov":10,"dec":11})

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def is_gov_au(host: str) -> bool:
    return host.endswith(".gov.au")

def limit_bytes(b: bytes, cap: int = 200_000) -> bytes:
    return b if len(b) <= cap else b[:cap]

def to_iso(dt: datetime) -> str:
    try:
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ""

def _parse_dayfirst_numeric(s: str) -> str:
    """Handle dd/mm/yyyy and dd.mm.yyyy and dd-mm-yyyy."""
    s = s.strip()
    for rx in (r"(\d{1,2})/(\d{1,2})/(\d{4})", r"(\d{1,2})\.(\d{1,2})\.(\d{4})", r"(\d{1,2})-(\d{1,2})-(\d{4})"):
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
    """Convert many date spellings into ISO (UTC) for comparison."""
    if not s:
        return ""
    s = s.strip()

    # Strip leading weekday names like "Thursday," or "Thu"
    s = re.sub(r'^(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?),?\s+', '', s, flags=re.I)

    # ISO-like
    try:
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        return to_iso(dt)
    except Exception:
        pass

    # Common formats
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
        if mon == "may": mon = "may2"
        if mon in MONTHS:
            dt = datetime(int(m.group(2)), MONTHS[mon]+1, 1, tzinfo=timezone.utc)
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

# ------------------ Date extraction ------------------
# Meta patterns
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
    re.compile(r'<meta[^>]+(itemprop|name|property)=["\']datePublished["\'][^>]+content=["\']([^"\']+)["\']', re.I),
    re.compile(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', re.I),
    re.compile(r'<meta[^>]+name=["\']dc\.date["\'][^>]+content=["\']([^"\']+)["\']', re.I),
]

# Visible text (generic)
WEEKDAY_OPT = r'(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\s+'
DATE_CORE   = r'(?:\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})'

VISIBLE_UPDATED_GENERIC = [
    re.compile(rf'\bLast\s*updated\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', re.I),
    re.compile(rf'\bPage\s*last\s*updated\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', re.I),
    re.compile(rf'\bLast\s*reviewed\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', re.I),
    re.compile(rf'\bReviewed\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', re.I),
    re.compile(rf'\bUpdated\s*(?:on)?\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', re.I),
    re.compile(rf'\bCurrent\s+as\s+at\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', re.I),
    re.compile(rf'\bLast\s*(?:changed|revised|modified)\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_CORE})', re.I),
]

# Fallback script-key search (for pages that stash dates in JS)
SCRIPT_KEYS = re.compile(
    r'"(?:dateModified|lastModified|lastUpdated|updatedAt|updated_at|modifiedAt|modified_at|pageUpdated|pageLastUpdated|dateLastUpdated|reviewDate|lastReviewed)"\s*:\s*"([^"]{4,30})"',
    re.I
)

def domain(host, *parts): return re.search("|".join([re.escape(p) for p in parts]), host, re.I)

def extract_dates(html: str, url: str):
    """
    Extract updated/published dates from:
      1) meta tags
      2) JSON-LD
      3) site-specific HTML patterns
      4) plain-text fallbacks (handles split tags)
      5) <time datetime="..."> with context
      6) script-key fallback (dateModified, lastUpdated, etc.)
    """
    updated = published = source = confidence = ""

    # Soup + plain text
    soup = BeautifulSoup(html or "", "lxml")
    for el in soup(["script","style","noscript"]): el.extract()
    text = re.sub(r"\s+", " ", (soup.get_text(" ", strip=True) or "").replace("\xa0"," ")).strip()

    # 1) Meta tags (high)
    for rx, src in META_UPDATED:
        m = rx.search(html)
        if m:
            updated = normalize_date(m.group(1)); source = src; confidence = "high"; break
    for rx in META_PUBLISHED:
        if not published:
            m = rx.search(html)
            if m:
                published = normalize_date(m.group(1 if rx is META_PUBLISHED[1] else (2 if rx is META_PUBLISHED[0] else 1)))

    # 2) JSON-LD (high)
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
                            updated = normalize_date(str(node["dateModified"])); source = source or "jsonld:dateModified"; confidence = confidence or "high"
                        if not updated and node.get("dateUpdated"):
                            updated = normalize_date(str(node["dateUpdated"])); source = source or "jsonld:dateUpdated"; confidence = confidence or "high"
                        if not published and node.get("datePublished"):
                            published = normalize_date(str(node["datePublished"]))
            if updated and not confidence: confidence = "high"

    # 3) Site-specific HTML hints (labels near dates)
    host = (urlparse(url).hostname or "").lower()
    def find_html(rx, src, conf):
        nonlocal updated, source, confidence
        if not updated:
            m = rx.search(html)
            if m:
                val = m.group(2) if (m.lastindex and m.lastindex >= 2) else m.group(1)
                updated = normalize_date(val)
                if updated:
                    source = source or src
                    confidence = confidence or conf

    # NSW Health – "Current as at"
    if "health.nsw.gov.au" in host:
    # Allow optional weekday before the actual date
    weekday_opt = r'(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\s+'
    date_core = r'(?:\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})'
    find_html(re.compile(rf'Current\s+as\s+at[^<:]*[:>]\s*(?:{weekday_opt})?({date_core})', re.I),
              "text:NSW:current-as-at", "high")
    # Meta fallbacks
    find_html(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified", "high")
    find_html(re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DCTERMS.modified", "high")

    # RACGP
    if "racgp.org.au" in host:
        find_html(re.compile(r'(Last\s*updated|Reviewed)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:RACGP", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified", "high")

    # Healthdirect/SA/ACI/eviQ/NICE/WHO/CDC/HETI/Monash/eTG (same as before but numeric dates allowed)
    # ... (reuse your previous patterns if needed; generic fallback will usually catch)

    # 4) Plain-text fallbacks (handles split tags)
    if not updated and text:
        for rx in VISIBLE_UPDATED_GENERIC:
            m = rx.search(text)
            if m:
                updated = normalize_date(m.group(1))
                if updated:
                    lab = rx.pattern.split("\\b")[1].lower().replace("\\s*", " ").replace("\\s+", " ")
                    source = source or f"text:{lab}"
                    confidence = confidence or ("high" if "updated" in lab or "current as at" in lab else "medium")
                    break

    # 5) <time datetime="..."> with 'updated/reviewed' context nearby
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
                    updated = val; source = source or "time:datetime"; confidence = confidence or "high"; break

    # 6) Script-key fallback for non-LD JSON blobs
    if not updated:
        m = SCRIPT_KEYS.search(html)
        if m:
            val = normalize_date(m.group(1)) or _parse_dayfirst_numeric(m.group(1)) or m.group(1)
            if val:
                updated = normalize_date(val); source = source or "script:date-key"; confidence = confidence or "medium"

    # Published fallback (visible text)
    if not published and text:
        for rx in [
            re.compile(r'\b(Published|Date\s*published|Publication\s*date|First\s*published)\b[:\s-]*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})', re.I),
        ]:
            m = rx.search(text)
            if m:
                published = normalize_date(m.group(2)); break

    return updated, published, source, confidence

# ------------------ Optional JS rendering ------------------
def should_render(host: str, html_text: str) -> bool:
    if not USE_PLAYWRIGHT: return False
    host = (host or "").lower()
    if RENDER_DOMAINS and not any(host.endswith(d) or d in host for d in RENDER_DOMAINS):
        return False
    # Heuristics: SPA shells or instructions to enable scripts
    if re.search(r'\bLoading\.\.\.|enable\s+scripts|this\s+site\s+requires\s+javascript', html_text or "", flags=re.I):
        return True
    # If nothing found by simple HTML (later we call when dates missing)
    return True

def render_html_playwright(url: str, timeout_ms: int = 25000) -> str:
    """Headless render using Playwright Chromium; returns fully rendered HTML."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        try:
            ctx = browser.new_context()
            page = ctx.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(url, wait_until="networkidle")
            # Give late JS a moment if needed
            with contextlib.suppress(Exception):
                page.wait_for_load_state("networkidle")
            content = page.content()
            return content or ""
        finally:
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
    etag = lastmod = title = content_hash = ""
    detected_updated = detected_published = updated_source = confidence = ""

    try:
        # Prefer HEAD then GET
        try:
            r = sess.head(url, allow_redirects=False, timeout=timeout)
        except Exception as e:
            notes.append(f"HEAD fail: {e.__class__.__name__}")
            r = sess.get(url, allow_redirects=False, timeout=timeout)

        # follow redirects (cap 5)
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
                soup = BeautifulSoup(body, "lxml")
                if soup.title and soup.title.string:
                    title = soup.title.string.strip()

                # Try static extraction first
                u,p,src,conf = extract_dates(html_text, final_url)

                # JS render fallback if still missing (or SPA shell detected)
                if (not u and not p) and should_render(host, html_text):
                    try:
                        rendered = render_html_playwright(final_url, timeout_ms=(timeout*1000))
                        if rendered:
                            u2,p2,src2,conf2 = extract_dates(rendered, final_url)
                            if not title:
                                s2 = BeautifulSoup(rendered, "lxml")
                                title = (s2.title.string.strip() if s2.title and s2.title.string else title)
                            # prefer rendered findings
                            if u2: u, src, conf = u2, src2, conf2 or conf
                            if p2: p = p2
                            notes.append("rendered:playwright")
                    except Exception as e:
                        notes.append(f"render fail: {e.__class__.__name__}")

                detected_updated, detected_published, updated_source, confidence = u,p,src,conf
                content_hash = sha256_hex(limit_bytes(body, 200_000))
            except Exception as e:
                notes.append(f"parse fail: {e.__class__.__name__}")

        elif code and 200 <= code < 300:
            try:
                if r.request.method == "HEAD":
                    rr = sess.get(final_url, allow_redirects=False, timeout=timeout, headers={"Range":"bytes=0-200000"})
                    body = rr.content or b""
                else:
                    body = r.content or b""
                content_hash = sha256_hex(limit_bytes(body, 200_000))
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
            etag=etag, lastmod=lastmod, content_hash=content_hash,
            det_updated=detected_updated, det_published=detected_published,
            upd_source=updated_source, confidence=confidence
        )
    except Exception as e:
        return dict(
            status="ERROR", code="", final_url=url, title="",
            notes=f"exception: {e.__class__.__name__}", etag="", lastmod="",
            content_hash="", det_updated="", det_published="",
            upd_source="", confidence=""
        )

# ------------------ Google Sheets I/O ------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
def gs_client():
    creds = Credentials.from_service_account_file("sa.json", scopes=SCOPES)
    return gspread.authorize(creds)

def read_rows(sheet_id, tab):
    gc = gs_client()
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    data = ws.get_all_values()
    if not data or len(data[0]) < 17:
        raise RuntimeError("Sheet headers (A→Q) are missing or incomplete.")
    return ws, data

def write_results(ws, rows, results):
    updates = []
    now = now_iso()
    for i, (row, res) in enumerate(zip(rows, results), start=2):
        while len(row) < 17:
            row.append("")
        prev_etag = row[7] if len(row) > 7 else ""
        prev_lastmod = row[8] if len(row) > 8 else ""
        prev_hash = row[9] if len(row) > 9 else ""
        prev_first_seen = row[11] if len(row) > 11 else ""
        prev_last_changed = row[12] if len(row) > 12 else ""

        changed = (
            (res["etag"] and res["etag"] != prev_etag) or
            (res["lastmod"] and res["lastmod"] != prev_lastmod) or
            (res["content_hash"] and res["content_hash"] != prev_hash)
        )
        first_seen = prev_first_seen or now
        last_changed = now if changed else (prev_last_changed or "")

        out = [
            res["status"],
            res["code"],
            res["final_url"],
            res["title"],
            now,
            res["notes"],
            res["etag"],
            res["lastmod"],
            res["content_hash"],
            "UPDATED" if changed else "",
            first_seen,
            last_changed,
            res["det_updated"],
            res["upd_source"],
            res["det_published"],
            res["confidence"],
        ]
        updates.append(out)

    end_row = 1 + len(results)
    ws.update(f"B2:Q{end_row}", updates, value_input_option="RAW")

def main():
    args = parse_args()
    ws, all_values = read_rows(args.sheet, args.tab)
    urls = [r[0].strip() for r in all_values[1:] if len(r) >= 1]
    rows = all_values[1:1+len(urls)]

    base_timeout = args.base_timeout
    gov_timeout = args.gov_timeout

    results = [None]*len(urls)
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {}
        for idx, url in enumerate(urls):
            if not url:
                results[idx] = dict(
                    status="SKIPPED", code="", final_url="", title="", notes="No URL",
                    etag="", lastmod="", content_hash="",
                    det_updated="", det_published="", upd_source="", confidence=""
                )
                continue
            futs[ex.submit(process_url, url, base_timeout, gov_timeout)] = idx

        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                results[idx] = fut.result()
            except Exception as e:
                results[idx] = dict(
                    status="ERROR", code="", final_url=urls[idx], title="",
                    notes=f"exception: {e.__class__.__name__}",
                    etag="", lastmod="", content_hash="",
                    det_updated="", det_published="", upd_source="", confidence=""
                )

    write_results(ws, rows, results)

if __name__ == "__main__":
    # Make DNS timeouts shorter to avoid long hangs
    socket.setdefaulttimeout(12)
    # Log to file for artifact
    import sys
    class Tee(object):
        def __init__(self, name):
            self.file = open(name, 'w', encoding='utf-8')
            self.stdout = sys.stdout
        def write(self, data):
            self.file.write(data); self.stdout.write(data)
        def flush(self):
            self.file.flush(); self.stdout.flush()
    sys.stdout = sys.stderr = Tee("run.log")
    main()
