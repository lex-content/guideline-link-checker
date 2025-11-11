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
