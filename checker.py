#!/usr/bin/env python3
import os
import re
import json
import hashlib
import argparse
import socket
import contextlib
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

# ===================== Config via CLI / Env =====================

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

# Alert strategies for Slack topic digest:
# - "both" (default): require text change AND date marker == today (local)
# - "today_only": require date marker == today (local)
# - "text_change": require text change (ignores dates)
ALERT_MODE = os.getenv("ALERT_MODE", "both").lower()
ALERT_TZ = os.getenv("ALERT_TZ", "Australia/Melbourne")

# ===================== HTTP Session =====================

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
        "User-Agent": "GH-Actions-LinkChecker/1.4 (+https://github.com/)",
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.8",
        "Cache-Control": "no-cache",
    })
    sess.request_timeout = timeout_s  # note: we still pass per-request timeouts
    return sess

# ===================== Date & Text Helpers =====================

MONTHS = {m:i for i,m in enumerate(
    ["january","february","march","april","may","june","july",
     "august","september","october","november","december"])}
MONTHS.update({"jan":0,"feb":1,"mar":2,"apr":3,"may2":4,"jun":5,"jul":6,"aug":7,"sep":8,"sept":8,"oct":9,"nov":10,"dec":11})

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def is_gov_au(host: str) -> bool:
    return (host or "").endswith(".gov.au")

def limit_bytes(b: bytes, cap: int = 200_000) -> bytes:
    if b is None:
        return b""
    return b if len(b) <= cap else b[:cap]

def normalize_date(s: str) -> str:
    """Parse many date spellings; return ISO-8601 UTC string or ''."""
    if not s:
        return ""
    s = s.strip()
    # Drop leading weekday like "Thursday," or "Thu "
    s = re.sub(
        r"^(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?),?\s+",
        "",
        s, flags=re.I
    )
    # ISO-ish
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # Common formats
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%b %d, %Y", "%B %d, %Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass
    # Month Year -> assume day 1
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{4})", s)
    if m:
        mon = m.group(1).lower()
        if mon == "may":
            mon = "may2"
        if mon in MONTHS:
            dt = datetime(int(m.group(2)), MONTHS[mon] + 1, 1, tzinfo=timezone.utc)
            return dt.isoformat()
    # yyyy-mm-dd anywhere
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        return dt.isoformat()
    # dd/mm/yyyy or dd.mm.yyyy or dd-mm-yyyy
    for rx in (r"(\d{1,2})/(\d{1,2})/(\d{4})", r"(\d{1,2})\.(\d{1,2})\.(\d{4})", r"(\d{1,2})-(\d{1,2})-(\d{4})"):
        m = re.search(rx, s)
        if m:
            try:
                dt = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)), tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
    return ""

# Remove volatile date/timestamp tokens from visible text
DATE_WORDS   = r"(?:Last\s*updated|Page\s*last\s*updated|Last\s*reviewed|Reviewed|Updated(?:\s*on)?|Current\s+as\s+at|Last\s*(?:changed|revised|modified))"
DATE_TOKEN   = r"(?:\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})(?:\s+\d{2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM))?)?"
TIMESTAMP_NOISE = re.compile(rf"{DATE_WORDS}\s*[:\-–—]?\s*{DATE_TOKEN}|{DATE_TOKEN}", re.I)

def stable_text_hash(html: str):
    """Hash of visible text with date-ish tokens stripped."""
    soup = BeautifulSoup(html or "", "lxml")
    for el in soup(["script", "style", "noscript"]):
        el.extract()
    text = soup.get_text(" ", strip=True) or ""
    norm = re.sub(r"\s+", " ", text).strip().lower()
    stable = re.sub(TIMESTAMP_NOISE, "", norm)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest(), text[:240]

def iso_to_local_day(iso_utc: str, tz_str: str) -> str:
    if not iso_utc:
        return ""
    try:
        dt = datetime.fromisoformat(iso_utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo(tz_str)).date().isoformat()
    except Exception:
        return ""

# ===================== Date Extraction =====================

META_UPDATED = [
    (re.compile(r"<meta[^>]+property=['\"]article:modified_time['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), "meta:article:modified_time"),
    (re.compile(r"<meta[^>]+property=['\"]og:updated_time['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), "meta:og:updated_time"),
    (re.compile(r"<meta[^>]+itemprop=['\"]dateModified['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), "meta:itemprop:dateModified"),
    (re.compile(r"<meta[^>]+name=['\"]last-modified['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), "meta:last-modified"),
    (re.compile(r"<meta[^>]+name=['\"]modified['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), "meta:modified"),
    (re.compile(r"<meta[^>]+name=['\"]dc\.date\.modified['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), "meta:dc.date.modified"),
    (re.compile(r"<meta[^>]+name=['\"]dcterms\.modified['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), "meta:DCTERMS.modified"),
    (re.compile(r"<meta[^>]+name=['\"]DC\.Date\.Modified['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), "meta:DC.Date.Modified"),
]
META_PUBLISHED = [
    (re.compile(r"<meta[^>]+(itemprop|name|property)=['\"]datePublished['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), 2),
    (re.compile(r"<meta[^>]+property=['\"]article:published_time['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), 1),
    (re.compile(r"<meta[^>]+name=['\"]dc\.date['\"][^>]+content=['\"]([^'\"]+)['\"]", re.I), 1),
]

def extract_dates_from_html(html: str, url: str):
    """
    Return: (det_updated_isoUTC, det_published_isoUTC, source, confidence, text_hash, excerpt, title)
    """
    updated = published = source = confidence = ""
    soup = BeautifulSoup(html or "", "lxml")
    for el in soup(["script", "style", "noscript"]):
        el.extract()
    text = re.sub(r"\s+", " ", (soup.get_text(" ", strip=True) or "").replace("\xa0", " ")).strip()

    # 1) Meta tags
    for rx, src in META_UPDATED:
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
        for script in re.findall(r"<script[^>]+type=['\"]application/ld\+json['\"][^>]*>([\s\S]*?)</script>", html, flags=re.I):
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

    # 3) Site-specific hints (NSW Health & RACGP)
    host = (urlparse(url).hostname or "").lower()

    if "health.nsw.gov.au" in host and not updated:
        rx = re.compile(
            r"(?:Current\s+as\s+at|Page\s+last\s+updated)[^<:]*[:>]\s*"
            r"(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*"
            r"(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})",
            re.I
        )
        m = rx.search(html)
        if m:
            updated = normalize_date(m.group(1)); source = "text:NSW:current-as-at"; confidence = "high"

    if "racgp.org.au" in host and not updated:
        rx = re.compile(
            r"(Last\s*updated|Reviewed)\s*:?<\/?[^>]*>\s*"
            r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})",
            re.I
        )
        m = rx.search(html)
        if m:
            updated = normalize_date(m.group(2)); source = "text:RACGP"; confidence = "medium"

    # 4) Plain-text fallbacks
    if not updated:
        for rx in [
            re.compile(r"\bLast\s*updated\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})", re.I),
            re.compile(r"\bPage\s*last\s*updated\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})", re.I),
            re.compile(r"\bLast\s*reviewed\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})", re.I),
            re.compile(r"\bReviewed\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})", re.I),
            re.compile(r"\bUpdated\s*(?:on)?\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})", re.I),
            re.compile(r"\bCurrent\s+as\s+at\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})", re.I),
            re.compile(r"\bLast\s*(?:changed|revised|modified)\b[:\s-]*(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|(?:\d{1,2}[/\.-]){2}\d{4}|\d{4}-\d{2}-\d{2})", re.I),
        ]:
            m = rx.search(text)
            if m:
                updated = normalize_date(m.group(1)); source = "text:last-updated"; confidence = "medium"; break

    # 5) <time datetime> with contextual words
    if not updated:
        for t in soup.find_all("time"):
            dt_attr = t.get("datetime") or t.get("content")
            context = " ".join(filter(None, [
                t.get_text(" ", strip=True),
                t.get("aria-label", ""),
                (t.parent.get_text(" ", strip=True) if t.parent else "")
            ])).lower()
            if dt_attr and re.search(r"updated|reviewed|modified|current as at", context):
                val = normalize_date(dt_attr)
                if val:
                    updated = val; source = "time:datetime"; confidence = "high"; break

    # 6) Script-key fallback
    if not updated:
        script_key_rx = re.compile(
            r"\"(?:dateModified|lastModified|lastUpdated|updatedAt|updated_at|modifiedAt|modified_at|pageUpdated|pageLastUpdated|dateLastUpdated|reviewDate|lastReviewed)\"\s*:\s*\"([^\"]{4,30})\"",
            re.I
        )
        m = script_key_rx.search(html)
        if m:
            val = normalize_date(m.group(1)) or m.group(1)
            if val:
                updated = normalize_date(val); source = "script:date-key"; confidence = "medium"

    # Title
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Visible text hash
    txt_hash, excerpt = stable_text_hash(html)
    return updated, published, source, confidence, txt_hash, excerpt, title

# ===================== Optional Rendering =====================

def should_render(host: str, html_text: str) -> bool:
    if not USE_PLAYWRIGHT:
        return False
    host = (host or "").lower()
    if RENDER_DOMAINS and not any(host.endswith(d) or d in host for d in RENDER_DOMAINS):
        return False
    # If it looks JS-driven, render; otherwise still render when enabled (conservative)
    if re.search(r"\bLoading\.\.\.|enable\s+javascript|enable\s+scripts|requires\s+javascript", html_text or "", re.I):
        return True
    return True

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
            return page.content() or ""
        finally:
            with contextlib.suppress(Exception):
                page.close()
            with contextlib.suppress(Exception):
                browser.close()

# ===================== Per-URL Processing =====================

def process_url(url: str, base_timeout: int, gov_timeout: int):
    host = (urlparse(url).hostname or "").lower()
    timeout = gov_timeout if is_gov_au(host) else base_timeout

    sess = make_session(timeout)
    notes = []
    code = 0
    final_url = url
    etag = lastmod = ""
    content_hash_raw = text_hash = text_excerpt = title = ""
    det_updated = det_published = upd_source = confidence = ""

    try:
        # Prefer HEAD then GET; follow redirects manually up to 5
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
        etag = r.headers.get("ETag", "")
        lastmod = r.headers.get("Last-Modified", "") or r.headers.get("last-modified", "")

        ct = (r.headers.get("Content-Type", "") or "").lower()
        body = b""
        html_text = ""

        if code and 200 <= code < 300 and "text/html" in ct:
            try:
                rr = r if r.request.method != "HEAD" else sess.get(final_url, allow_redirects=False, timeout=timeout)
                body = rr.content or b""
                html_text = rr.text or ""

                u, p, src, conf, t_hash, excerpt, t_title = extract_dates_from_html(html_text, final_url)
                if t_title: title = t_title

                # If no dates found OR page likely JS, render (if enabled)
                if (not u and not p) and should_render(host, html_text):
                    try:
                        rendered = render_html_playwright(final_url, timeout_ms=int(timeout * 1000))
                        if rendered:
                            u2, p2, src2, conf2, t_hash2, excerpt2, t_title2 = extract_dates_from_html(rendered, final_url)
                            if u2: u, src, conf = u2, src2, conf2 or conf
                            if p2: p = p2
                            if t_title2: title = t_title2
                            if t_hash2: t_hash, excerpt = t_hash2, excerpt2
                            notes.append("rendered:playwright")
                    except Exception as e:
                        notes.append(f"render fail: {e.__class__.__name__}")

                det_updated, det_published, upd_source, confidence = u, p, src, conf
                content_hash_raw = sha256_hex(limit_bytes(body, 200_000))
                text_hash, text_excerpt = t_hash or "", excerpt or ""
            except Exception as e:
                notes.append(f"parse fail: {e.__class__.__name__}")

        elif code and 200 <= code < 300:
            # Non-HTML (e.g., PDF) — hash first bytes for reference
            try:
                if r.request.method == "HEAD":
                    rr = sess.get(final_url, allow_redirects=False, timeout=timeout, headers={"Range": "bytes=0-200000"})
                    body = rr.content or b""
                else:
                    body = r.content or b""
                content_hash_raw = sha256_hex(limit_bytes(body, 200_000))
            except Exception as e:
                notes.append(f"hash fail: {e.__class__.__name__}")

        # Classify
        if code and 200 <= code < 300:
            status = "OK"; note = " | ".join(n for n in notes if n)
        elif code:
            status = "BROKEN"
            reason = ("Auth or access restricted" if code in (401, 403) else
                      "Rate limited" if code == 429 else
                      "Server error" if code >= 500 else
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
            "status": "ERROR", "code": "", "final_url": url, "title": "",
            "notes": f"exception: {e.__class__.__name__}", "etag": "", "lastmod": "",
            "content_hash": "", "det_updated": "", "det_published": "",
            "upd_source": "", "confidence": "", "text_hash": "", "text_excerpt": ""
        }

# ===================== Google Sheets I/O =====================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def gs_client():
    creds = Credentials.from_service_account_file("sa.json", scopes=SCOPES)
    return gspread.authorize(creds)

def ensure_col_s(ws):
    """Ensure Column S exists and header is set. Column R is the user's Topic Area (untouched)."""
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
    """Write B→Q (core) and S (text hash). Leave Column R untouched."""
    now_iso_str = now_utc_iso()
    core_updates = []
    text_hash_updates = []

    for row, res in zip(rows, results):
        row = (row + [""] * (19 - len(row)))[:19]
        prev_text_hash = row[18] or ""
        prev_last_changed = row[12] or ""
        prev_first_seen = row[11] or ""

        new_text_hash = res.get("text_hash", "")
        changed_text = bool(prev_text_hash) and bool(new_text_hash) and (new_text_hash != prev_text_hash)

        last_changed = now_iso_str if changed_text else prev_last_changed
        first_seen = prev_first_seen or now_iso_str

        core_updates.append([
            res.get("status", ""),           # B
            res.get("code", ""),             # C
            res.get("final_url", ""),        # D
            res.get("title", ""),            # E
            now_iso_str,                     # F Last Checked
            res.get("notes", ""),            # G Notes
            res.get("etag", ""),             # H ETag
            res.get("lastmod", ""),          # I Last-Modified
            res.get("content_hash", ""),     # J Content Hash (raw)
            "UPDATED" if changed_text else "",  # K Change? (text-hash)
            first_seen,                      # L First Seen
            last_changed,                    # M Last Changed
            res.get("det_updated", ""),      # N Detected Updated
            res.get("upd_source", ""),       # O Updated Source
            res.get("det_published", ""),    # P Detected Published
            res.get("confidence", ""),       # Q Parse Confidence
        ])
        text_hash_updates.append([new_text_hash])      # S

    end_row = 1 + len(rows)
    if rows:
        ws.update(f"B2:Q{end_row}", core_updates, value_input_option="RAW")
        ws.update(f"S2:S{end_row}", text_hash_updates, value_input_option="RAW")

# ===================== Summaries (Slack topic digest) =====================

def _sanitize_title_for_slack(t: str) -> str:
    return (t or "").replace("|", "∣").strip()

def _best_local_day(lastmod_iso, det_updated_iso, det_published_iso, tz_str) -> str:
    for iso in (det_updated_iso, lastmod_iso, det_published_iso):
        if not iso:
            continue
        day = iso_to_local_day(iso, tz_str)
        if day:
            return day
    return ""

def _write_topic_area_summary(rows, results, tz_str: str):
    """
    Build Slack text grouped by Column R ('Topic Area').

    Inclusion rule:
      - ALWAYS include if Column K would be UPDATED this run (i.e., visible-text hash changed).
      - OTHERWISE, fall back to ALERT_MODE for date-driven inclusion:
          * "today_only": include if best date marker == today
          * "both":       (since no text change) include only if best date marker == today
          * "text_change": exclude (no text change)
    """
    today = datetime.now(ZoneInfo(tz_str)).date().isoformat()
    groups = OrderedDict()
    total_changes = 0

    for row, res in zip(rows, results):
        row = (row + [""] * (19 - len(row)))[:19]
        url_a = (row[0] or "").strip()
        if not url_a:
            continue

        status = (res.get("status") or "").upper()
        if status != "OK":
            continue

        # previous vs new
        prev_text_hash = row[18] or ""
        prev_lastmod   = row[8]  or ""
        prev_det_upd   = row[13] or ""
        prev_pub       = row[15] or ""
        prev_src       = row[14] or ""
        prev_raw_hash  = row[9]  or ""

        new_text_hash  = res.get("text_hash","")
        new_lastmod    = res.get("lastmod","")
        new_det_upd    = res.get("det_updated","")
        new_pub        = res.get("det_published","")
        new_src        = res.get("upd_source","")
        new_raw_hash   = res.get("content_hash","")
        title          = (res.get("title") or row[4] or url_a).strip()

        # Column K logic: would this be UPDATED this run?
        changed_text = bool(prev_text_hash) and bool(new_text_hash) and (new_text_hash != prev_text_hash)

        # Best local date marker for "today" checks
        def _best_local_day(lastmod_iso, det_updated_iso, det_published_iso, tz):
            for iso in (det_updated_iso, lastmod_iso, det_published_iso):
                if not iso:
                    continue
                d = iso_to_local_day(iso, tz)
                if d:
                    return d
            return ""
        best_day = _best_local_day(new_lastmod, new_det_upd, new_pub, tz_str)
        date_is_today = (best_day == today) if best_day else False

        # Inclusion gate:
        include = False
        if changed_text:
            # ALWAYS include K-updates
            include = True
        else:
            # No text change — decide based on alert mode (date-driven)
            if ALERT_MODE == "text_change":
                include = False
            else:
                # "today_only" or "both": include if date marker is today
                include = bool(date_is_today)

        if not include:
            continue

        # Tag only the labels that ALSO changed this run
        tags = []
        if new_lastmod and new_lastmod != prev_lastmod:
            tags.append(f"modified: {new_lastmod}")
        if new_raw_hash and new_raw_hash != prev_raw_hash:
            tags.append("changed")
        if new_det_upd and new_det_upd != prev_det_upd:
            tags.append(f"updated: {new_det_upd}")
        if new_src and new_src != prev_src:
            tags.append(f"source updated: {new_src}")
        if new_pub and new_pub != prev_pub:
            tags.append(f"published: {new_pub}")

        topic = (row[17] or "").strip() or "Uncategorised"  # Column R
        groups.setdefault(topic, []).append({
            "title": _sanitize_title_for_slack(title),
            "url": url_a,
            "tags": tags,
        })
        total_changes += 1

    # Build Slack message text (your requested format)
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

    with open("topic_summary.json", "w", encoding="utf-8") as f:
        json.dump({"date_local": today, "total_changes": total_changes, "groups": groups}, f, ensure_ascii=False, indent=2)
    with open("topic_summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) if lines else "")

        def gate():
            if ALERT_MODE == "text_change":
                return changed_text
            if ALERT_MODE == "today_only":
                return date_is_today
            # default: "both"
            return changed_text and date_is_today

        if not gate():
            continue

        # Build tags for labels that ALSO changed this run
        tags = []
        if new_lastmod and new_lastmod != prev_lastmod:
            tags.append(f"modified: {new_lastmod}")
        if new_raw_hash and new_raw_hash != prev_raw_hash:
            tags.append("changed")
        if new_det_updated and new_det_updated != prev_det_updated:
            tags.append(f"updated: {new_det_updated}")
        if new_src and new_src != prev_upd_source:
            tags.append(f"source updated: {new_src}")
        if new_published and new_published != prev_published:
            tags.append(f"published: {new_published}")

        topic = (row[17] or "").strip() or "Uncategorised"  # Column R
        groups.setdefault(topic, []).append({
            "title": _sanitize_title_for_slack(title),
            "url": url_a,
            "tags": tags,
        })
        total_changes += 1

    # Build Slack text in your requested format
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

    with open("topic_summary.json", "w", encoding="utf-8") as f:
        json.dump({"date_local": today, "total_changes": total_changes, "groups": groups}, f, ensure_ascii=False, indent=2)
    with open("topic_summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) if lines else "")

def _write_summary_files(rows, results):
    """Generic counts (not used for alerting; helpful for sanity checks)."""
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

    with open("summary.json", "w", encoding="utf-8") as f:
        json.dump({"date_local": date_label, "counts": {"checked": checked, "updated": updated, "broken": broken, "errors": errors}}, f, ensure_ascii=False, indent=2)
    with open("summary.md", "w", encoding="utf-8") as f:
        f.write(f"*Daily guideline link check — {date_label}*\n\n"
                f"*Totals:* {checked} checked · {updated} updated · {broken} broken · {errors} errors\n")

# ===================== Main =====================

def main():
    args = parse_args()

    ws, data = read_rows(args.sheet, args.tab)
    rows = data[1:]
    urls = [(r[0].strip() if len(r) >= 1 else "") for r in rows]

    results = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {}
        for idx, url in enumerate(urls):
            if not url:
                results[idx] = {
                    "status": "SKIPPED", "code": "", "final_url": "", "title": "", "notes": "No URL",
                    "etag": "", "lastmod": "", "content_hash": "",
                    "det_updated": "", "det_published": "",
                    "upd_source": "", "confidence": "", "text_hash": "", "text_excerpt": ""
                }
                continue
            futs[ex.submit(process_url, url, args.base_timeout, args.gov_timeout)] = idx

        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                results[idx] = fut.result()
            except Exception as e:
                results[idx] = {
                    "status": "ERROR", "code": "", "final_url": urls[idx], "title": "",
                    "notes": f"exception: {e.__class__.__name__}", "etag": "", "lastmod": "",
                    "content_hash": "", "det_updated": "", "det_published": "",
                    "upd_source": "", "confidence": "", "text_hash": "", "text_excerpt": ""
                }

    # Build summaries BEFORE writing (so comparisons use prior sheet state)
    _write_summary_files(rows, results)
    _write_topic_area_summary(rows, results, ALERT_TZ)

    # Persist back to the sheet
    write_results(ws, rows, results)

# ===================== Runner =====================

if __name__ == "__main__":
    # Faster DNS failure
    socket.setdefaulttimeout(12)

    # Mirror logs to file for GitHub artifact
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
