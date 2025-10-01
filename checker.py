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
