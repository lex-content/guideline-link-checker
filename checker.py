import os, re, json, time, hashlib, argparse, socket
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials

# --------- config via CLI/env ----------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet", required=True, help="Google Sheet ID")
    ap.add_argument("--tab", required=True, help="Worksheet name (tab)")
    ap.add_argument("--workers", type=int, default=int(os.getenv("MAX_WORKERS", "6")))
    ap.add_argument("--gov-timeout", type=int, default=int(os.getenv("GOV_TIMEOUT", "30")))
    ap.add_argument("--base-timeout", type=int, default=int(os.getenv("BASE_TIMEOUT", "15")))
    return ap.parse_args()

# --------- HTTP session with retries ----------
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
        "User-Agent": "GH-Actions-LinkChecker/1.0 (+https://github.com/)",
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.8",
        "Cache-Control": "no-cache",
    })
    sess.request_timeout = timeout_s
    return sess

# --------- helpers ----------
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

def normalize_date(s: str) -> str:
    if not s: return ""
    s = s.strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        return to_iso(dt)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%b %d, %Y", "%B %d, %Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return to_iso(dt)
        except Exception:
            pass
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{4})", s)
    if m:
        mon = m.group(1).lower()
        if mon == "may": mon = "may2"
        if mon in MONTHS:
            dt = datetime(int(m.group(2)), MONTHS[mon]+1, 1, tzinfo=timezone.utc)
            return to_iso(dt)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        return to_iso(dt)
    return ""

# --------- date extraction (general + site-specific) ----------
META_UPDATED = [
    (re.compile(r'<meta[^>]+property=["\']article:modified_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:article:modified_time"),
    (re.compile(r'<meta[^>]+property=["\']og:updated_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:og:updated_time"),
    (re.compile(r'<meta[^>]+itemprop=["\']dateModified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:itemprop:dateModified"),
    (re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified"),
    (re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified"),
    (re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DCTERMS.modified"),
]
META_PUBLISHED = re.compile(r'<meta[^>]+(itemprop|name|property)=["\']datePublished["\'][^>]+content=["\']([^"\']+)["\']', re.I)

VISIBLE_UPDATED_GENERIC = [
    re.compile(r'Last\s*updated\s*[:-]\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', re.I),
    re.compile(r'Last\s*updated\s*[:-]\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})', re.I),
    re.compile(r'Updated\s*[:-]\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', re.I),
    re.compile(r'Last\s*reviewed\s*[:-]\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', re.I),
    re.compile(r'Reviewed\s*[:-]\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', re.I),
    re.compile(r'Updated\s*on\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', re.I),
    re.compile(r'Last\s*updated\s*on\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})', re.I),
    re.compile(r'Last\s*updated\s*[:-]\s*(\d{4}-\d{2}-\d{2})', re.I),
]

def domain(host, *parts): return re.search("|".join([re.escape(p) for p in parts]), host, re.I)

def extract_dates(html: str, url: str):
    """
    Extract updated/published dates from HTML with:
    1) meta tags
    2) JSON-LD
    3) site-specific HTML patterns
    4) NEW: plain-text fallbacks (handles 'Last updated' split across tags)
    5) Generic <time datetime="..."> with 'updated' context
    """
    updated = published = source = confidence = ""

    # --- Make soup + plain text early so we can reuse both ---
    soup = BeautifulSoup(html, "lxml")
    for el in soup(["script", "style", "noscript"]):
        el.extract()
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()

    # --- 1) Meta tags (high confidence) ---
    for rx, src in META_UPDATED:
        m = rx.search(html)
        if m:
            updated = normalize_date(m.group(1)); source = src; confidence = "high"; break
    m = META_PUBLISHED.search(html)
    if m and not published:
        published = normalize_date(m.group(2))

    # --- 2) JSON-LD (high confidence) ---
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
            if updated and published and not confidence:
                confidence = "high"

    # --- 3) Site-specific HTML patterns (as before) ---
    host = (urlparse(url).hostname or "").lower()
    def find_html(rx, src, conf):
        nonlocal updated, source, confidence
        if not updated:
            m = rx.search(html)
            if m:
                # prefer the last group as the date if pattern has 2 groups (label + date)
                val = m.group(2) if m.lastindex and m.lastindex >= 2 else m.group(1)
                updated = normalize_date(val)
                source = source or src
                confidence = confidence or conf

    # eviQ
    if re.search(r"(?:^|\.)eviq\.org\.au|(?:^|\.)cancerinstitute\.org\.au$", host):
        find_html(re.compile(r'Last\s*updated\s*:?<\/?[^>]*>\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})', re.I), "text:eviQ", "high")
    # NHMRC
    if "nhmrc.gov.au" in host:
        find_html(re.compile(r'Date\s+last\s+updated\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{4})', re.I), "text:NHMRC", "medium")
    # ACSQHC
    if "safetyandquality.gov.au" in host or "acsqhc.gov.au" in host:
        find_html(re.compile(r'Last\s*updated\s*:?<\/?[^>]*>\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:ACSQHC", "medium")
    # MAGICapp
    if "magicapp.org" in host:
        find_html(re.compile(r'(Last\s*updated|Last\s*modified)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})', re.I), "text:MAGICapp", "medium")
    # Healthdirect (+ meta)
    if "healthdirect.gov.au" in host:
        find_html(re.compile(r'(Last\s*reviewed|Page\s*last\s*updated|Last\s*updated)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:Healthdirect", "high")
        find_html(re.compile(r'<meta[^>]+name=["\']date\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:date.modified", "high")
    # SA Health (+ meta)
    if "sahealth.sa.gov.au" in host or host.endswith("sa.gov.au"):
        find_html(re.compile(r'(Last\s*updated|Last\s*changed|Date\s*last\s*updated)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:SA Health", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']DC\.Date\.Modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DC.Date.Modified", "high")
        find_html(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified", "high")
    # NSW Health / ACI (+ meta)
    if "health.nsw.gov.au" in host or "aci.health.nsw.gov.au" in host:
        find_html(re.compile(r'(Last\s*updated|Date\s*last\s*updated)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:NSW Health/ACI", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified", "high")
        find_html(re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DCTERMS.modified", "high")
    # RACGP (+ meta)
    if "racgp.org.au" in host:
        find_html(re.compile(r'(Last\s*updated|Reviewed)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:RACGP", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified", "high")
    # eTG
    if any(s in host for s in ["tg.org.au", "etg.tg.org.au", "tgldcdp.tg.org.au", "tgld.tg.org.au"]):
        find_html(re.compile(r'(Last\s*updated|Last\s*revised|Content\s*last\s*updated)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:eTG", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified", "high")
    # HETI
    if "heti.nsw.gov.au" in host or "learn.heti.nsw.gov.au" in host:
        find_html(re.compile(r'(Last\s*updated|Last\s*reviewed|Date\s*updated)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:HETI", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified", "high")
    # Monash Health
    if "monashhealth.org" in host:
        find_html(re.compile(r'(Last\s*updated|Reviewed|Page\s*updated)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:MonashHealth", "medium")
        find_html(re.compile(r'<meta[^>]+name=["\']article:modified_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:article:modified_time", "high")
    # WHO
    if host.endswith("who.int"):
        find_html(re.compile(r'<meta[^>]+name=["\']lastmod["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:lastmod", "high")
        find_html(re.compile(r'(Last\s*updated|Last\s*reviewed)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:WHO", "medium")
    # CDC
    if host.endswith("cdc.gov"):
        find_html(re.compile(r'(Page\s*last\s*reviewed|Page\s*last\s*updated|Last\s*updated)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:CDC", "high")
    # NICE
    if host.endswith("nice.org.uk"):
        find_html(re.compile(r'(Last\s*updated|Updated)\s*:?<\/?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:NICE", "medium")
        find_html(re.compile(r'<time[^>]+datetime=["\']([^"\']+)["\'][^>]*>\s*(?:Last\s*updated|Updated)?', re.I), "time:datetime", "high")

    # --- 4) NEW: plain-text fallbacks (handles split tags/extra markup) ---
    if not updated and text:
        text_patterns = [
            re.compile(r'\b(Last\s*updated|Page\s*last\s*updated|Last\s*reviewed|Last\s*modified|Date\s*last\s*updated|Updated(?:\s*on)?)\b[:\s]*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I),
        ]
        for rx in text_patterns:
            m = rx.search(text)
            if m:
                updated = normalize_date(m.group(2))
                label = m.group(1).lower().replace(" ", "-")
                source = source or f"text:{label}"
                # mark high when it explicitly says "last updated"/"page last updated"
                confidence = confidence or ("high" if "updated" in label else "medium")
                break

    # --- 5) Generic <time datetime="..."> with 'updated' context in nearby text ---
    if not updated:
        for t in soup.find_all("time"):
            dt_attr = t.get("datetime") or t.get("content")
            context = " ".join(filter(None, [
                t.get_text(" ", strip=True),
                t.get("aria-label", ""),
                (t.parent.get_text(" ", strip=True) if t.parent else "")
            ])).lower()
            if dt_attr and re.search(r'updated|reviewed|modified', context):
                val = normalize_date(dt_attr)
                if val:
                    updated = val; source = source or "time:datetime"; confidence = confidence or "high"; break

    # --- Generic published fallback from plain text (if still missing) ---
    if not published and text:
        for rx in [
            re.compile(r'\b(Published|Date\s*published|Publication\s*date)\b[:\s]*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I),
        ]:
            m = rx.search(text)
            if m:
                published = normalize_date(m.group(2)); break

    return updated, published, source, confidence

    def find(rx, src, conf):
        nonlocal updated, source, confidence
        if not updated:
            m = rx.search(html)
            if m:
                updated = normalize_date(m.group(1) if len(m.groups())==1 else m.group(2))
                source = source or src
                confidence = confidence or conf

    if domain(host, "eviq.org.au", "cancerinstitute.org.au"):
        find(re.compile(r'Last\s*updated\s*:?\s*</?[^>]*>\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})', re.I), "text:eviQ", "high")
    if domain(host, "nhmrc.gov.au"):
        find(re.compile(r'Date\s+last\s+updated\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+[0-9]{4})', re.I), "text:NHMRC", "medium")
    if domain(host, "safetyandquality.gov.au", "acsqhc.gov.au"):
        find(re.compile(r'Last\s*updated\s*:?\s*</?[^>]*>\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:ACSQHC", "medium")
    if domain(host, "magicapp.org"):
        find(re.compile(r'(Last\s*updated|Last\s*modified)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})', re.I), "text:MAGICapp", "medium")
    if domain(host, "healthdirect.gov.au"):
        find(re.compile(r'(Last\s*reviewed|Page\s*last\s*updated|Last\s*updated)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:Healthdirect", "high")
        find(re.compile(r'<meta[^>]+name=["\']date\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:date.modified", "high")
    if domain(host, "sahealth.sa.gov.au", "sa.gov.au"):
        find(re.compile(r'(Last\s*updated|Last\s*changed|Date\s*last\s*updated)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:SA Health", "medium")
        find(re.compile(r'<meta[^>]+name=["\']DC\.Date\.Modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DC.Date.Modified", "high")
        find(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified", "high")
    if domain(host, "health.nsw.gov.au", "aci.health.nsw.gov.au"):
        find(re.compile(r'(Last\s*updated|Date\s*last\s*updated)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:NSW Health/ACI", "medium")
        find(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified", "high")
        find(re.compile(r'<meta[^>]+name=["\']dcterms\.modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:DCTERMS.modified", "high")
    if domain(host, "racgp.org.au"):
        find(re.compile(r'(Last\s*updated|Reviewed)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:RACGP", "medium")
        find(re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified", "high")
    if domain(host, "tg.org.au", "etg.tg.org.au", "tgldcdp.tg.org.au", "tgld.tg.org.au"):
        find(re.compile(r'(Last\s*updated|Last\s*revised|Content\s*last\s*updated)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:eTG", "medium")
        find(re.compile(r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:last-modified", "high")
    if domain(host, "heti.nsw.gov.au", "learn.heti.nsw.gov.au"):
        find(re.compile(r'(Last\s*updated|Last\s*reviewed|Date\s*updated)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:HETI", "medium")
        find(re.compile(r'<meta[^>]+name=["\']modified["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:modified", "high")
    if domain(host, "monashhealth.org"):
        find(re.compile(r'(Last\s*updated|Reviewed|Page\s*updated)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:MonashHealth", "medium")
        find(re.compile(r'<meta[^>]+name=["\']article:modified_time["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:article:modified_time", "high")
    if domain(host, "who.int"):
        find(re.compile(r'<meta[^>]+name=["\']lastmod["\'][^>]+content=["\']([^"\']+)["\']', re.I), "meta:lastmod", "high")
        find(re.compile(r'(Last\s*updated|Last\s*reviewed)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:WHO", "medium")
    if domain(host, "cdc.gov"):
        find(re.compile(r'(Page\s*last\s*reviewed|Page\s*last\s*updated|Last\s*updated)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:CDC", "high")
    if domain(host, "nice.org.uk"):
        find(re.compile(r'(Last\s*updated|Updated)\s*:?\s*</?[^>]*>\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{4}-\d{2}-\d{2})', re.I), "text:NICE", "medium")
        find(re.compile(r'<time[^>]+datetime=["\']([^"\']+)["\'][^>]*>\s*(?:Last\s*updated|Updated)?', re.I), "time:datetime", "high")

    if not updated:
        for rx in VISIBLE_UPDATED_GENERIC:
            m = rx.search(html)
            if m:
                updated = normalize_date(m.group(1))
                source = source or "text:last-updated"
                confidence = confidence or "medium"
                break
    if not published:
        for rx in [
            re.compile(r'Published\s*[:-]\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', re.I),
            re.compile(r'Date\s+published\s*[:-]\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', re.I),
            re.compile(r'Publication\s+date\s*[:-]\s*(\d{4}-\d{2}-\d{2})', re.I),
        ]:
            m = rx.search(html)
            if m:
                published = normalize_date(m.group(1))
                break
    return updated, published, source, confidence

# --------- per-URL processing ----------
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
        if code and 200 <= code < 300 and "text/html" in ct:
            try:
                rr = r if r.request.method != "HEAD" else sess.get(final_url, allow_redirects=False, timeout=timeout)
                body = rr.content or b""
                soup = BeautifulSoup(body, "lxml")
                if soup.title and soup.title.string:
                    title = soup.title.string.strip()
                u,p,src,conf = extract_dates(rr.text, final_url)
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

# --------- Google Sheets I/O ----------
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
    socket.setdefaulttimeout(12)
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
