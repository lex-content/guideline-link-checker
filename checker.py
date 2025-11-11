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

# for summaries / Slack text
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

# ---------- Smarter normalize_date (strips weekday prefixes) ----------
def normalize_date(s: str) -> str:
    """Convert many date spellings into ISO (UTC) for comparison."""
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
# Published meta patterns with explicit group index
META_PUBLISHED = [
    (re.compile(r'<meta[^>]+(itemprop|name|property)=["\']datePublished["\'][^>]+content=["\']([^"\']+)["\']', re.I), 2),
    (re.compile(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), 1),
    (re.compile(r'<meta[^>]+name=["\']dc\.date["\'][^>]+content=["\']([^"\']+)["\']', re.I), 1),
]

# Generic visible-text patterns with optional weekday
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
    for rx, gi in META_PUBLISHED:
        if not published:
            m = rx.search(html)
            if m:
                published = normalize_date(m.group(gi))

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

    # NSW Health – "Current as at" with optional weekday
    if "health.nsw.gov.au" in host:
        weekday_opt = r'(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\s+'
        date_core = DATE_CORE
        find_html(re.compile(rf'Current\s+as\s+at[^<:]*[:>]\s*(?:{weekday_opt})?({date_core})', re.I),
                  "text:NSW:current-as-at", "high")
        # Meta fallbacks
        find_html(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified", "high")
        find_html(re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DCTERMS.modified", "high")

    # RACGP (example)
    if "racgp.org.au" in host:
        find_html(re.compile(r'(Last\s*updated|Reviewed)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:RACGP", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified", "high")

    # 4) Plain-text fallbacks (handles split tags)
    if not updated and text:
        for rx in VISIBLE_UPDATED_GENERIC:
            m = rx.search(text)
            if m:
                updated = normalize_date(m.group(1))
                if updated:
                    source = source or "text:last-updated"
                    confidence = confidence or "medium"
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
    # If nothing found by simple HTML (we call only when dates missing anyway)
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
            res["status"],           # B
            res["code"],             # C
            res["final_url"],        # D
            res["title"],            # E
            now,                     # F Last Checked
            res["notes"],            # G
            res["etag"],             # H
            res["lastmod"],          # I
            res["content_hash"],     # J
            "UPDATED" if changed else "",  # K Change?
            first_seen,              # L First Seen
            last_changed,            # M Last Changed
            res["det_updated"],      # N Detected Updated
            res["upd_source"],       # O Updated Source
            res["det_published"],    # P Detected Published
            res["confidence"],       # Q Parse Confidence
        ]
        updates.append(out)

    end_row = 1 + len(results)
    ws.update(f"B2:Q{end_row}", updates, value_input_option="RAW")

# ------------------ (Existing) Simple Summary (kept for compatibility) ------------------
def _generate_summary(rows, results, tz_str="Australia/Melbourne"):
    tz = ZoneInfo(tz_str)
    now_local = datetime.now(tz)
    date_label = now_local.strftime("%A %d %B %Y")

    items_updated, items_broken, items_error = [], [], []
    for row, res in zip(rows, results):
        row = (row + [""] * (17 - len(row)))[:17]
        url = row[0].strip()
        title = res.get("title") or row[4] or url
        final_url = res.get("final_url") or url
        det_upd = res.get("det_updated") or ""
        upd_src = res.get("upd_source") or ""
        status  = (res.get("status") or "").upper()

        if status == "BROKEN":
            items_broken.append({"title": title, "url": final_url, "code": res.get("code"), "notes": res.get("notes")})
        elif status == "ERROR":
            items_error.append({"title": title, "url": final_url, "notes": res.get("notes")})
        else:
            prev_etag = row[7] or ""
            prev_lastmod = row[8] or ""
            prev_hash = row[9] or ""
            changed = (
                (res.get("etag") and res["etag"] != prev_etag) or
                (res.get("lastmod") and res["lastmod"] != prev_lastmod) or
                (res.get("content_hash") and res["content_hash"] != prev_hash)
            )
            if changed:
                items_updated.append({
                    "title": title, "url": final_url,
                    "det_updated": det_upd, "upd_source": upd_src,
                })

    summary = {
        "date_local": date_label,
        "counts": {
            "checked": len(results),
            "updated": len(items_updated),
            "broken": len(items_broken),
            "errors": len(items_error),
        },
        "updated": items_updated,
        "broken": items_broken,
        "errors": items_error,
    }
    return summary

def _write_summary_files(rows, results):
    summary = _generate_summary(rows, results)
    with open("summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    lines = [f"*Daily guideline link check — {summary['date_local']}*", "",
             f"*Totals:* {summary['counts']['checked']} checked · {summary['counts']['updated']} updated · {summary['counts']['broken']} broken · {summary['counts']['errors']} errors", ""]
    if summary["updated"]:
        lines.append("*Updated*")
        for it in summary["updated"][:10]:
            extra = ""
            if it.get('det_updated') or it.get('upd_source'):
                extra = f" · _{it.get('det_updated','')}_"
                if it.get('upd_source'): extra += f" via `{it['upd_source']}`"
            lines.append(f"• <{it['url']}|{it['title']}>{extra}")
        lines.append("")
    if summary["broken"]:
        lines.append("*Broken*")
        for it in summary["broken"][:10]:
            code = f" [{it.get('code')}]" if it.get("code") else ""
            lines.append(f"• <{it['url']}|{it['title']}>{code}")
        lines.append("")
    if summary["errors"]:
        lines.append("*Errors*")
        for it in summary["errors"][:10]:
            lines.append(f"• <{it['url']}|{it['title']}>")
        lines.append("")
    with open("summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# ------------------ NEW: Topic Area (Column R) Summary — pretty Slack formatting ------------------
def _sanitize_title_for_slack(t: str) -> str:
    return (t or "").replace("|", "∣").strip()

def _write_topic_area_summary(rows, results, tz_str="Australia/Melbourne"):
    """
    Builds a Slack-ready message grouped by Column R ("Topic Area"),
    ONLY for rows that had a NEW change this run, with formatting like:

    Here's the daily update on any new key resource changes:

    Topic Area: *Addiction (❗️6 changes detected)*
    • <URL|Title>, modified: Thu, 02 Oct 2025 00:42:21 GMT, changed
    """
    tz = ZoneInfo(tz_str)
    now_local = datetime.now(tz)
    date_label = now_local.strftime("%A %d %B %Y")

    groups = OrderedDict()   # {topic_area: [ {title,url,tags[]} ]}
    total_changes = 0

    for row, res in zip(rows, results):
        # Ensure columns up to R exist (A=0 .. R=17)
        row = (row + [""] * (18 - len(row)))[:18]
        url_colA = (row[0] or "").strip()
        if not url_colA:
            continue

        # Previous saved values (from sheet)
        prev_etag        = row[7]  or ""   # H
        prev_lastmod     = row[8]  or ""   # I
        prev_hash        = row[9]  or ""   # J
        prev_det_updated = row[13] or ""   # N
        prev_upd_source  = row[14] or ""   # O
        prev_published   = row[15] or ""   # P
        topic_area       = (row[17] or "").strip() or "Uncategorised"  # R

        # New values from this run
        lastmod     = res.get("lastmod")        or ""
        contenthash = res.get("content_hash")   or ""
        det_updated = res.get("det_updated")    or ""
        upd_source  = res.get("upd_source")     or ""
        det_pub     = res.get("det_published")  or ""

        # Detect NEW changes relative to previous sheet values
        changed_modified   = bool(lastmod)     and (lastmod     != prev_lastmod)
        changed_hash       = bool(contenthash) and (contenthash != prev_hash)      # 'changed'
        changed_updated    = bool(det_updated) and (det_updated != prev_det_updated)
        changed_upd_source = bool(upd_source)  and (upd_source  != prev_upd_source)
        changed_published  = bool(det_pub)     and (det_pub     != prev_published)

        if not (changed_modified or changed_hash or changed_updated or changed_upd_source or changed_published):
            continue  # skip rows with no new change this run

        # Compose labels (only those that changed this run), in your phrasing/order
        tags = []
        if changed_modified:   tags.append(f"modified: {lastmod}")
        if changed_hash:       tags.append("changed")
        if changed_updated:    tags.append(f"updated: {det_updated}")
        if changed_upd_source: tags.append(f"source updated: {upd_source}")
        if changed_published:  tags.append(f"published: {det_pub}")

        # Title: Column E preferred, else detected title, else URL (and sanitise pipes for Slack)
        title_colE = (row[4] or "").strip()
        title = _sanitize_title_for_slack(title_colE or res.get("title") or url_colA)

        groups.setdefault(topic_area, []).append({
            "title": title,
            "url": url_colA,        # Use Column A URL for the hyperlink
            "tags": tags,
        })
        total_changes += 1

    # Build Slack text in your exact style
    lines = []
    if total_changes:
        lines.append("Here's the daily update on any new key resource changes:")
        lines.append("")  # blank line after intro

        for topic, items in groups.items():
            if not items:
                continue
            n = len(items)
            change_word = "change" if n == 1 else "changes"
            # Bold topic header with ❗️ and count
            lines.append(f"Topic Area: *{topic} (❗️{n} {change_word} detected)*")

            # Bulleted items with clickable titles
            for it in items:
                link = f"<{it['url']}|{it['title']}>"
                suffix = f", {', '.join(it['tags'])}" if it["tags"] else ""
                lines.append(f"• {link}{suffix}")
            lines.append("")  # spacer between groups

    summary = {
        "date_local": date_label,
        "total_changes": total_changes,
        "groups": groups,
    }
    with open("topic_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    # Write the final Slack text
    with open("topic_summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) if lines else "")

# ------------------ Main ------------------
def main():
    # Defer import of heavy helper to avoid NameError from chat formatting
    global extract_all_from_html
    def extract_all_from_html(html, url):
        # Reuse previously defined pieces
        updated, published, source, confidence, = "", "", "", ""
        soup = BeautifulSoup(html or "", "lxml")
        for el in soup(["script","style","noscript"]): el.extract()
        text = re.sub(r"\s+", " ", (soup.get_text(" ", strip=True) or "").replace("\xa0"," ")).strip()

        # meta
        for rx, src in META_UPDATED:
            m = rx.search(html)
            if m:
                updated = normalize_date(m.group(1)); source = src; confidence = "probable"; break
        for rx, gi in META_PUBLISHED:
            if not published:
                m = rx.search(html)
                if m:
                    published = normalize_date(m.group(gi))

        # JSON-LD
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

        host = (urlparse(url).hostname or "").lower()
        if "health.nsw.gov.au" in host and not updated:
            weekday_opt = r'(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\s+'
            date_core = r'(?:\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d2)'
            m = re.search(rf'Current\s+as\s+at[^<:]*[:>]\s*(?:{weekday_opt})?({date_core})', html, re.I)
            if m:
                updated = normalize_date(m.group(1)); source = "text:NSW:current-as-at"; confidence = "high"

        if not updated:
            for rx in [
                re.compile(rf'\bLast\s*updated\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_TOKEN})', re.I),
                re.compile(rf'\bPage\s*last\s*updated\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_TOKEN})', re.I),
                re.compile(rf'\bLast\s*reviewed\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_TOKEN})', re.I),
                re.compile(rf'\bReviewed\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_TOKEN})', re.I),
                re.compile(rf'\bUpdated\s*(?:on)?\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_TOKEN})', re.I),
                re.compile(rf'\bCurrent\s+as\s+at\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_TOKEN})', re.I),
                re.compile(rf'\bLast\s*(?:changed|revised|modified)\b[:\s-]*(?:{WEEKDAY_OPT})?({DATE_TOKEN})', re.I),
            ]:
                mm = rx.search(text)
                if mm:
                    updated = normalize_date(mm.group(1)); source = "text:last-updated"; confidence = "medium"; break

        if not published:
            m = re.search(r'\b(Published|Date\s*published|Publication\s*date|First\s*published)\b[:\s-]*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})', text, re.I)
            if m:
                published = normalize_date(m.group(2))

        # text hash
        try:
            text_hash, excerpt = text_hash_from_html(html)
        except Exception:
            text_hash, excerpt = "", ""

        return updated, published, source, confidence, text_hash, excerpt

    ws, all_values = read_rows(args.sheet, args.tab)
    urls = [r[0].strip() for r in all_values[1:] if len(r) >= 1]
    rows = all_values[1:1+len(urls)]

    base_timeout = args.base_timeout
    gov_timeout = args.gov_timeout

    results = [None]*len(urls)
    from bs4 import BeautifulSoup as _BS  # ensure available after potential reimport
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
                    det_updated="", det_published="",
                    upd_source="", confidence="",
                    text_hash="", text_excerpt=""
                )

    # Build & write summaries BEFORE persisting (so we compare against previous sheet values)
    _write_summary_files(rows, results)      # generic rollup (optional)
    _write_topic_area_summary(rows, results) # Topic Area digest (content-change only)

    # Persist core results and text hash; leaves Column R untouched
    write_results(ws, rows, results)

if __name__ == "__main__":
    # Make DNS lookups fail fast
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
