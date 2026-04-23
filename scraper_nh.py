"""
Scraper for New Hampshire DOJ – Security Breach Notifications
URL: https://doj.nh.gov/citizens/consumer-protection-antitrust-bureau/security-breach-notifications

Run standalone:  python scraper_nh.py
Imported by:     run_daily.py  (calls scrape() -> list[dict])
"""

import json
import logging
import os
import re
import subprocess
import time

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser
from datetime import date

# ─── Configuration ────────────────────────────────────────────────────────────

BASE_URL       = "https://doj.nh.gov"
PAGE_URL       = "https://doj.nh.gov/citizens/consumer-protection-antitrust-bureau/security-breach-notifications"
SEEN_URLS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nh_seen_urls.json")
DEBUG_HTML_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nh_last_response_debug.html")
CUTOFF_DATE    = date(2026, 1, 1)
SOURCE_NAME    = "New Hampshire DOJ"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Cache-Control":   "no-cache",
    "Pragma":          "no-cache",
}

BROWSER_HEADERS = {
    **HEADERS,
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
}

_DATE_RE = re.compile(
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b"
    r"|\b\d{4}-\d{2}-\d{2}\b"
    r"|\b(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s+\d{4}\b",
    re.IGNORECASE,
)
_PDF_URL_RE = re.compile(
    r'https?://(?:www\.)?mm\.nh\.gov/files/uploads/doj/remote-docs/[^"\'>\s\\]+?\.pdf',
    re.IGNORECASE,
)

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scraper_nh")


# ─── State management ─────────────────────────────────────────────────────────

def load_seen_urls() -> set:
    if os.path.exists(SEEN_URLS_FILE):
        try:
            with open(SEEN_URLS_FILE, "r") as fh:
                data = json.load(fh)
            return set(data) if isinstance(data, list) else set()
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read seen URLs file %s: %s", SEEN_URLS_FILE, exc)
            return set()
    return set()


def save_seen_urls(urls: set) -> None:
    with open(SEEN_URLS_FILE, "w") as fh:
        json.dump(sorted(urls), fh, indent=2)
    logger.info("Saved %d seen URLs to %s", len(urls), SEEN_URLS_FILE)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def resolve_url(href: str) -> str:
    href = href.strip()
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return BASE_URL + href
    return BASE_URL + "/" + href


def try_parse_date(text: str):
    """Return a date object or None."""
    if not text:
        return None
    try:
        return dateutil_parser.parse(text.strip(), fuzzy=True).date()
    except Exception:
        return None


def find_nearest_date(element) -> str:
    """Walk up the DOM from element looking for a date string."""
    node = element
    for _ in range(4):
        if node is None:
            break
        text = node.get_text(separator=" ", strip=True)
        m = _DATE_RE.search(text)
        if m:
            return m.group(0)
        node = node.parent
    return ""


def is_nav_link(href: str, text: str) -> bool:
    skip_fragments = ["#", "mailto:", "tel:", "javascript:"]
    skip_paths = ["login", "logout", "search", "sitemap", "contact",
                  "home", "about", "privacy", "accessibility", "faq",
                  "facebook", "twitter", "linkedin"]
    if not href or not text:
        return True
    if any(href.startswith(f) for f in skip_fragments):
        return True
    href_lower = href.lower()
    if any(k in href_lower for k in skip_paths):
        return True
    if len(text.strip()) < 4:
        return True
    return False


def _entity_from_pdf_url(url: str) -> str:
    filename = url.rstrip("/").split("/")[-1]
    stem = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    stem = re.sub(r"-\d{8}$", "", stem)
    return stem.replace("-", " ").replace("_", " ").strip().title() or filename


def _date_from_pdf_url(url: str) -> str:
    filename = url.rstrip("/").split("/")[-1]
    match = re.search(r"(\d{4})(\d{2})(\d{2})(?=\.pdf$)", filename, re.IGNORECASE)
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{month}/{day}/{year}"


# ─── Fetch ────────────────────────────────────────────────────────────────────

def fetch_page(url: str):
    """Fetch a page and return a BeautifulSoup object, or None on failure."""
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)
    try:
        resp = session.get(url, timeout=30, allow_redirects=True)
        logger.info("NH fetch via requests: %s -> %s (%d)",
                    url, resp.url, resp.status_code)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as exc:
        logger.warning("Requests fetch failed for %s: %s", url, exc)

    curl_cmd = [
        "/usr/bin/curl",
        "--silent",
        "--show-error",
        "--location",
        "--compressed",
        "--max-time", "45",
        "--user-agent", BROWSER_HEADERS["User-Agent"],
        "--header", f"Accept: {BROWSER_HEADERS['Accept']}",
        "--header", f"Accept-Language: {BROWSER_HEADERS['Accept-Language']}",
        "--header", "Upgrade-Insecure-Requests: 1",
        "--header", "Sec-Fetch-Site: none",
        "--header", "Sec-Fetch-Mode: navigate",
        "--header", "Sec-Fetch-User: ?1",
        "--header", "Sec-Fetch-Dest: document",
        url,
    ]
    try:
        result = subprocess.run(
            curl_cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.info("NH fetch via curl succeeded for %s", url)
        return BeautifulSoup(result.stdout, "html.parser")
    except (OSError, subprocess.CalledProcessError) as exc:
        logger.error("Curl fetch failed for %s: %s", url, exc)
        return None


# ─── Parsing strategies ───────────────────────────────────────────────────────

def _parse_table(soup) -> list[dict]:
    """Strategy 1 – <table> elements."""
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        header_cells = rows[0].find_all(["th", "td"])
        col_map = {}
        for i, th in enumerate(header_cells):
            txt = th.get_text(strip=True).lower()
            if any(k in txt for k in ["company", "entity", "organization", "name", "business", "title"]):
                col_map[i] = "entity"
            elif any(k in txt for k in ["date", "filed", "received", "noticed", "reported", "notified"]):
                col_map[i] = "date"

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            link = row.find("a", href=True)
            if not link:
                continue

            entity = ""
            date_str = ""
            for i, cell in enumerate(cells):
                cell_text = cell.get_text(separator=" ", strip=True)
                role = col_map.get(i)
                if role == "entity":
                    entity = cell_text
                elif role == "date":
                    date_str = cell_text
                else:
                    if not date_str and _DATE_RE.search(cell_text):
                        date_str = cell_text
                    elif not entity and link in cell.find_all("a"):
                        entity = cell_text

            if not entity:
                entity = link.get_text(strip=True)

            records.append({
                "entity":   entity or "Unknown",
                "date_str": date_str.strip(),
                "url":      resolve_url(link["href"]),
                "source":   SOURCE_NAME,
            })

        if records:
            logger.info("Strategy 1 (table): found %d rows", len(records))
            return records
    return []


def _parse_definition_list(soup) -> list[dict]:
    """
    Strategy 2 – <dl>/<dt>/<dd> pattern.
    NH DOJ pages sometimes list notices as definition lists:
      <dt>Company Name (MM/DD/YYYY)</dt><dd><a href="...">Letter</a></dd>
    """
    records = []
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            link = dd.find("a", href=True) or dt.find("a", href=True)
            if not link:
                continue
            dt_text = dt.get_text(separator=" ", strip=True)
            # Extract date from dt text
            m = _DATE_RE.search(dt_text)
            date_str = m.group(0) if m else ""
            # Entity = dt text minus the date portion
            entity = _DATE_RE.sub("", dt_text).strip(" -()")
            if not entity:
                entity = link.get_text(strip=True)

            records.append({
                "entity":   entity or "Unknown",
                "date_str": date_str,
                "url":      resolve_url(link["href"]),
                "source":   SOURCE_NAME,
            })

    if records:
        logger.info("Strategy 2 (definition list): found %d items", len(records))
    return records


def _parse_paragraph_links(soup) -> list[dict]:
    """
    Strategy 3 – paragraph-level links.
    NH DOJ often formats each notice as a <p> containing a link and date:
      <p><a href="...">Company Name</a> – MM/DD/YYYY</p>
    or as <li> within <ul>.
    """
    records = []
    main = (
        soup.find("main")
        or soup.find(id=re.compile(r"main[-_]?content|content|page[-_]?content", re.I))
        or soup.find(class_=re.compile(r"\bmain[-_]?content\b|\bcontent[-_]?area\b", re.I))
        or soup.find("article")
        or soup.find("body")
    )
    if not main:
        return []

    # Collect <p> and <li> elements that contain at least one anchor
    containers = main.find_all(["p", "li"])
    for container in containers:
        link = container.find("a", href=True)
        if not link:
            continue
        href = link["href"]
        text = link.get_text(strip=True)
        if is_nav_link(href, text):
            continue

        container_text = container.get_text(separator=" ", strip=True)
        m = _DATE_RE.search(container_text)
        date_str = m.group(0) if m else find_nearest_date(link)

        records.append({
            "entity":   text,
            "date_str": date_str.strip(),
            "url":      resolve_url(href),
            "source":   SOURCE_NAME,
        })

    if records:
        logger.info("Strategy 3 (paragraph/list links): found %d items", len(records))
    return records


def _parse_accordion_or_section(soup) -> list[dict]:
    """
    Strategy 4 – accordion / expandable sections.
    Some government CMS pages use Bootstrap accordions or custom collapsibles.
    Each panel header contains the breach name/date; the body has the link.
    """
    records = []
    # Bootstrap accordion panels
    panels = soup.find_all(class_=re.compile(r"panel|accordion|collapse|card", re.I))
    for panel in panels:
        link = panel.find("a", href=True)
        header = panel.find(class_=re.compile(r"panel-title|card-title|heading|accordion-header", re.I))
        if not link:
            continue

        heading_text = header.get_text(strip=True) if header else panel.get_text(separator=" ", strip=True)
        m = _DATE_RE.search(heading_text)
        date_str = m.group(0) if m else ""
        entity = _DATE_RE.sub("", heading_text).strip(" -()") or link.get_text(strip=True)

        if is_nav_link(link["href"], entity):
            continue

        records.append({
            "entity":   entity or "Unknown",
            "date_str": date_str,
            "url":      resolve_url(link["href"]),
            "source":   SOURCE_NAME,
        })

    if records:
        logger.info("Strategy 4 (accordion/sections): found %d items", len(records))
    return records


def _parse_generic_links(soup) -> list[dict]:
    """Strategy 5 – Generic fallback: all meaningful links in content area."""
    records = []
    main = (
        soup.find("main")
        or soup.find(id=re.compile(r"main[-_]?content|content|body-content", re.I))
        or soup.find(class_=re.compile(r"\bmain[-_]?content\b|\bcontent\b", re.I))
        or soup.find("body")
    )
    if not main:
        return []

    seen_in_page = set()
    for link in main.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if is_nav_link(href, text):
            continue
        url = resolve_url(href)
        if url in seen_in_page:
            continue
        seen_in_page.add(url)

        date_str = find_nearest_date(link)
        records.append({
            "entity":   text,
            "date_str": date_str.strip(),
            "url":      url,
            "source":   SOURCE_NAME,
        })

    if records:
        logger.info("Strategy 5 (generic fallback): found %d links", len(records))
    return records


def _is_pdf_url(url: str) -> bool:
    """Return True if the URL points to a PDF file."""
    return url.lower().split("?")[0].endswith(".pdf")


def _extract_pdf_urls_from_raw_html(soup) -> list[dict]:
    """Fallback: scan raw HTML for embedded PDF URLs, including JSON/script blobs."""
    html = str(soup)
    html = html.replace("\\/", "/")

    urls = []
    seen = set()
    for url in _PDF_URL_RE.findall(html):
        clean_url = url.replace("&amp;", "&")
        if clean_url in seen:
            continue
        seen.add(clean_url)
        urls.append({
            "entity": _entity_from_pdf_url(clean_url),
            "date_str": _date_from_pdf_url(clean_url),
            "url": clean_url,
            "source": SOURCE_NAME,
        })

    if urls:
        logger.info("Strategy 6 (raw HTML PDF scan): found %d embedded PDF URLs", len(urls))
    return urls


def extract_records(soup) -> list[dict]:
    """Try each strategy in order; return first result that contains PDF links."""
    for strategy in (
        _parse_table,
        _parse_definition_list,
        _parse_paragraph_links,
        _parse_accordion_or_section,
        _parse_generic_links,
        _extract_pdf_urls_from_raw_html,
    ):
        records = strategy(soup)
        if records:
            pdf_records = [r for r in records if _is_pdf_url(r["url"])]
            if pdf_records:
                logger.info("Filtered to %d PDF records (from %d total)", len(pdf_records), len(records))
                return pdf_records
            logger.debug("Strategy found %d records but none were PDF links; trying next strategy", len(records))
    try:
        with open(DEBUG_HTML_FILE, "w", encoding="utf-8") as fh:
            fh.write(str(soup))
        logger.warning("No PDF records found on page – saved debug HTML to %s", DEBUG_HTML_FILE)
    except OSError as exc:
        logger.warning("No PDF records found on page and could not save debug HTML: %s", exc)
    return []


# ─── Pagination ───────────────────────────────────────────────────────────────

def get_next_page_url(soup, current_url: str):
    nxt = soup.find("a", rel="next")
    if not nxt:
        nxt = soup.find("a", string=re.compile(r"^\s*(next|›|»)\s*$", re.I))
    if nxt:
        href = nxt.get("href", "")
        if href:
            return resolve_url(href)

    # Bootstrap/WordPress pagination
    pager = soup.find(class_=re.compile(r"next|pager[-_]?next", re.I))
    if pager:
        a = pager.find("a", href=True) if pager.name != "a" else pager
        if a:
            return resolve_url(a.get("href", ""))

    return None


# ─── Filter & deduplication ───────────────────────────────────────────────────

def filter_new(records: list[dict], seen_urls: set) -> tuple[list[dict], set]:
    """
    Remove already-seen and pre-cutoff records.
    Returns (new_records, updated_seen_urls).
    """
    new_records = []
    updated = set(seen_urls)

    for rec in records:
        url = rec["url"]
        if url in updated:
            continue

        parsed_date = try_parse_date(rec.get("date_str", ""))
        if parsed_date is not None and parsed_date < CUTOFF_DATE:
            logger.debug("Skip %s – date %s before cutoff", url, parsed_date)
            updated.add(url)
            continue

        rec["date_iso"] = parsed_date.isoformat() if parsed_date else ""
        new_records.append(rec)
        updated.add(url)

    return new_records, updated


# ─── Main entry point ─────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    """
    Scrape NH DOJ breach notifications.
    Returns a list of new records (not previously seen, on/after cutoff date).
    Each record: {entity, date_str, date_iso, url, source}
    """
    logger.info("=== NH scrape started ===")
    seen_urls = load_seen_urls()
    logger.info("Loaded %d previously seen URLs", len(seen_urls))

    all_records: list[dict] = []
    current_url = PAGE_URL
    page_num = 1

    while current_url and page_num <= 20:
        logger.info("Fetching page %d: %s", page_num, current_url)
        soup = fetch_page(current_url)
        if not soup:
            if page_num == 1:
                raise ConnectionError(f"NH DOJ page unreachable: {current_url}")
            break

        page_records = extract_records(soup)
        all_records.extend(page_records)
        logger.info("Page %d: %d records", page_num, len(page_records))

        next_url = get_next_page_url(soup, current_url)
        if not next_url or next_url == current_url:
            break
        current_url = next_url
        page_num += 1
        time.sleep(1)

    logger.info("Total raw records across all pages: %d", len(all_records))

    new_records, updated_seen = filter_new(all_records, seen_urls)
    save_seen_urls(updated_seen)

    logger.info("=== NH scrape complete: %d new records ===", len(new_records))
    return new_records


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    results = scrape()
    print(f"\nNew NH breach notifications found: {len(results)}")
    for r in results:
        label = f"[{r['date_str']}]" if r["date_str"] else "[no date]"
        print(f"  {label}  {r['entity']}")
        print(f"           {r['url']}")
