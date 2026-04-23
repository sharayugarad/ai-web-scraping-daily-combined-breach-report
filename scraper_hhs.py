"""
Scraper for HHS OCR – HIPAA Breach Report Portal
URL: https://ocrportal.hhs.gov/ocr/breach/breach_report_hip.jsf

This is a JavaServer Faces (JSF) / PrimeFaces application.
Pagination is driven by AJAX POST requests carrying javax.faces.ViewState.

Run standalone:  python scraper_hhs.py
Imported by:     run_daily.py  (calls scrape() -> list[dict])
"""

import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import date

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

# ─── Configuration ────────────────────────────────────────────────────────────

PORTAL_URL     = "https://ocrportal.hhs.gov/ocr/breach/breach_report_hip.jsf"
SEEN_KEYS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hhs_seen_keys.json")
CUTOFF_DATE    = date(2026, 1, 1)
SOURCE_NAME    = "HHS OCR"
MAX_PAGES      = 50

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

AJAX_HEADERS = {
    **HEADERS,
    "Accept":           "application/xml, text/xml, */*; q=0.01",
    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    "Faces-Request":    "partial/ajax",
    "X-Requested-With": "XMLHttpRequest",
    "Origin":           "https://ocrportal.hhs.gov",
    "Referer":          PORTAL_URL,
}

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scraper_hhs")


# ─── State management ─────────────────────────────────────────────────────────

def load_seen_keys() -> set:
    if os.path.exists(SEEN_KEYS_FILE):
        try:
            with open(SEEN_KEYS_FILE, "r") as fh:
                data = json.load(fh)
            return set(data) if isinstance(data, list) else set()
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read seen keys file %s: %s", SEEN_KEYS_FILE, exc)
            return set()
    return set()


def save_seen_keys(keys: set) -> None:
    with open(SEEN_KEYS_FILE, "w") as fh:
        json.dump(sorted(keys), fh, indent=2)
    logger.info("Saved %d seen keys to %s", len(keys), SEEN_KEYS_FILE)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def try_parse_date(text: str):
    """Return a date object or None."""
    if not text:
        return None
    try:
        return dateutil_parser.parse(text.strip(), fuzzy=True).date()
    except Exception:
        return None


def _make_key(entity: str, state: str, date_str: str) -> str:
    """Composite deduplication key (no per-record URLs on this portal)."""
    return f"{entity.strip().lower()}|{state.strip().lower()}|{date_str.strip()}"


# ─── JSF / PrimeFaces helpers ─────────────────────────────────────────────────

def _extract_viewstate(soup) -> str:
    """Extract javax.faces.ViewState from a BeautifulSoup page."""
    tag = soup.find("input", {"name": "javax.faces.ViewState"})
    if tag:
        return tag.get("value", "")
    # Some JSF implementations render it inside a script block
    scripts = soup.find_all("script")
    for s in scripts:
        m = re.search(r"javax\.faces\.ViewState['\"]?\s*[,:=]\s*['\"]([^'\"]+)['\"]", s.get_text())
        if m:
            return m.group(1)
    return ""


def _score_table(table) -> tuple[int, int]:
    """Score tables by how closely their headers match the breach grid."""
    header_row = table.find("tr")
    if not header_row:
        return (0, 0)

    headers = [
        th.get_text(separator=" ", strip=True)
        for th in header_row.find_all(["th", "td"])
    ]
    if not headers:
        return (0, 0)

    matched_fields = set()
    for header in headers:
        for field, pattern in _COL_PATTERNS.items():
            if pattern.search(header):
                matched_fields.add(field)
                break

    row_count = max(len(table.find_all("tr")) - 1, 0)
    return (len(matched_fields), row_count)


def _find_results_table(soup):
    """Return the most likely breach-results table from the page, or None."""
    best_table = None
    best_score = (0, 0)

    for table in soup.find_all("table"):
        score = _score_table(table)
        if score > best_score:
            best_table = table
            best_score = score

    if best_score[0] >= 4:
        return best_table
    return None


def _detect_form_and_table(soup) -> tuple[str, str]:
    """Return (form_id, table_id) for the main breach report form."""
    form_id = ""
    table_id = ""

    results_table = _find_results_table(soup)
    if results_table:
        form = results_table.find_parent("form")
        if form and form.get("id"):
            form_id = form["id"]

        # PrimeFaces often wraps the table in a div/span with the widget id.
        for tag in [results_table, *results_table.parents]:
            if not getattr(tag, "get", None):
                continue
            candidate_id = tag.get("id", "")
            if candidate_id and re.search(r"table|grid|report|breach|results", candidate_id, re.I):
                table_id = candidate_id
                break

    if not form_id:
        for form in soup.find_all("form"):
            if form.get("id"):
                form_id = form.get("id")
                break

    return form_id, table_id


def _detect_rows_per_page(soup) -> int:
    """Try to detect the rows-per-page setting; default 100."""
    # PrimeFaces paginator rowsPerPageTemplate or aria-rowcount
    tag = soup.find(attrs={"data-rpp": True})
    if tag:
        try:
            return int(tag["data-rpp"])
        except (ValueError, TypeError):
            pass
    # Look for a rows-per-page dropdown
    rpp_select = soup.find("select", id=re.compile(r"rows|rpp|pageSize", re.I))
    if rpp_select:
        selected = rpp_select.find("option", selected=True)
        if selected:
            try:
                return int(selected.get_text(strip=True))
            except ValueError:
                pass
    return 100


# ─── Fetch helpers ────────────────────────────────────────────────────────────

def _initial_fetch(session: requests.Session) -> tuple:
    """
    GET the portal page.
    Returns (soup, viewstate, form_id, table_id) or raises on failure.
    """
    logger.info("Fetching initial HHS OCR portal page: %s", PORTAL_URL)
    resp = session.get(PORTAL_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    viewstate = _extract_viewstate(soup)
    if not viewstate:
        logger.warning("Could not extract ViewState from initial page")
    form_id, table_id = _detect_form_and_table(soup)
    logger.info("Detected form_id=%r  table_id=%r", form_id, table_id)

    return soup, viewstate, form_id, table_id


# ─── Table parsing ────────────────────────────────────────────────────────────

# Regex patterns for each logical column
_COL_PATTERNS = {
    "entity":               re.compile(r"name of covered entity|^name$|organization|company", re.I),
    "state":                re.compile(r"\bstate\b", re.I),
    "covered_entity_type":  re.compile(r"type of covered|covered entity type|entity type", re.I),
    "individuals_affected":  re.compile(r"individual|affected|number", re.I),
    "date_str":             re.compile(r"date|submitted|reported|filed|notified", re.I),
    "breach_type":          re.compile(r"type of breach|breach type", re.I),
    "location":             re.compile(r"location|where|info\s*location", re.I),
    "ba_present":           re.compile(r"business\s*associate|b\.?a\.?|associate", re.I),
}


def _parse_table_html(source) -> list[dict]:
    """
    Parse an HHS OCR breach table from either a BeautifulSoup object or
    a raw HTML string.  Returns a list of record dicts.
    """
    if isinstance(source, str):
        soup = BeautifulSoup(source, "html.parser")
    else:
        soup = source

    table = _find_results_table(soup)
    if not table:
        return []

    # Build column index map from <th> headers
    header_row = table.find("tr")
    if not header_row:
        return []

    col_map: dict[int, str] = {}
    for i, th in enumerate(header_row.find_all(["th", "td"])):
        txt = th.get_text(separator=" ", strip=True)
        for field, pattern in _COL_PATTERNS.items():
            if pattern.search(txt):
                col_map[i] = field
                break

    records = []
    all_rows = table.find_all("tr")
    for row in all_rows[1:]:   # skip header
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        rec: dict = {
            "entity":              "",
            "state":               "",
            "covered_entity_type": "",
            "individuals_affected": "",
            "date_str":            "",
            "date_iso":            "",
            "breach_type":         "",
            "location":            "",
            "ba_present":          "",
            "url":                 PORTAL_URL,
            "source":              SOURCE_NAME,
        }

        for i, cell in enumerate(cells):
            field = col_map.get(i)
            if field:
                rec[field] = cell.get_text(separator=" ", strip=True)

        # If col_map is empty (no matched headers), try positional fallback
        if not col_map and len(cells) >= 5:
            rec["entity"]              = cells[0].get_text(strip=True)
            rec["state"]               = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            rec["covered_entity_type"] = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            rec["individuals_affected"] = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            rec["date_str"]            = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            rec["breach_type"]         = cells[5].get_text(strip=True) if len(cells) > 5 else ""
            rec["location"]            = cells[6].get_text(strip=True) if len(cells) > 6 else ""
            rec["ba_present"]          = cells[7].get_text(strip=True) if len(cells) > 7 else ""

        if rec["entity"]:
            records.append(rec)

    return records


def _parse_results_text(source) -> list[dict]:
    """Fallback parser for cases where the results grid is rendered as text."""
    if isinstance(source, str):
        soup = BeautifulSoup(source, "html.parser")
    else:
        soup = source

    text = soup.get_text("\n", strip=True)
    if "Breach Report Results" not in text:
        return []

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    records = []
    row_re = re.compile(
        r"^(?P<entity>.+?)\s+"
        r"(?P<state>[A-Z]{2})\s+"
        r"(?P<covered_entity_type>Healthcare Provider|Health Plan|Business Associate)\s+"
        r"(?P<individuals_affected>[\d,]+)\s+"
        r"(?P<date_str>\d{2}/\d{2}/\d{4})\s+"
        r"(?P<tail>.+?)\s+"
        r"(?P<ba_present>Yes|No)$"
    )

    for line in lines:
        match = row_re.match(line)
        if not match:
            continue
        rec = match.groupdict()
        rec["breach_type"] = rec.pop("tail")
        rec["location"] = ""
        rec["date_iso"] = ""
        rec["url"] = PORTAL_URL
        rec["source"] = SOURCE_NAME
        records.append(rec)

    return records


# ─── PrimeFaces AJAX pagination ───────────────────────────────────────────────

def _ajax_next_page(
    session:       requests.Session,
    viewstate:     str,
    form_id:       str,
    table_id:      str,
    first_row:     int,
    rows_per_page: int,
) -> tuple[list[dict], str]:
    """
    POST a PrimeFaces AJAX pagination request.
    Returns (records, new_viewstate).
    """
    data: dict[str, str] = {
        "javax.faces.partial.ajax":    "true",
        "javax.faces.source":          table_id,
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render":  table_id,
        table_id:                      table_id,
        f"{table_id}_pagination":      "true",
        f"{table_id}_first":           str(first_row),
        f"{table_id}_rows":            str(rows_per_page),
        "javax.faces.ViewState":       viewstate,
    }
    # Include the form id itself (JSF requirement)
    if form_id:
        data[form_id] = form_id

    resp = session.post(PORTAL_URL, data=data, headers=AJAX_HEADERS, timeout=30)
    resp.raise_for_status()
    return _parse_partial_response(resp.text)


def _parse_partial_response(xml_text: str) -> tuple[list[dict], str]:
    """
    Parse a PrimeFaces XML partial-response.
    Returns (records, new_viewstate).

    Response format:
      <?xml ...?>
      <partial-response>
        <changes>
          <update id="FORM:TABLE"><![CDATA[...HTML table...]]></update>
          <update id="j_id1:javax.faces.ViewState:1"><![CDATA[new_vs]]></update>
        </changes>
      </partial-response>
    """
    records:   list[dict] = []
    new_vs:    str        = ""

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.error("Failed to parse XML partial response: %s", exc)
        return records, new_vs

    for update in root.iter("update"):
        uid  = update.get("id", "")
        text = update.text or ""

        # ViewState update
        if "ViewState" in uid:
            new_vs = text.strip()
            continue

        # Table HTML update – look for actual <table> markup
        if "<table" in text.lower():
            records = _parse_table_html(text)
            if not records:
                records = _parse_results_text(text)

    return records, new_vs


# ─── Filter & deduplication ───────────────────────────────────────────────────

def filter_new(records: list[dict], seen_keys: set) -> tuple[list[dict], set]:
    """
    Remove already-seen and pre-cutoff records.
    Returns (new_records, updated_seen_keys).
    """
    new_records = []
    updated = set(seen_keys)

    for rec in records:
        key = _make_key(rec["entity"], rec["state"], rec["date_str"])
        if key in updated:
            continue

        parsed_date = try_parse_date(rec.get("date_str", ""))
        if parsed_date is not None and parsed_date < CUTOFF_DATE:
            logger.debug("Skip %r – date %s before cutoff", rec["entity"], parsed_date)
            updated.add(key)
            continue

        rec["date_iso"] = parsed_date.isoformat() if parsed_date else ""
        new_records.append(rec)
        updated.add(key)

    return new_records, updated


# ─── Main entry point ─────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    """
    Scrape HHS OCR breach notifications.
    Returns a list of new records (not previously seen, on/after cutoff date).
    Each record: {entity, state, covered_entity_type, individuals_affected,
                  date_str, date_iso, breach_type, location, ba_present, url, source}
    """
    logger.info("=== HHS scrape started ===")
    seen_keys = load_seen_keys()
    logger.info("Loaded %d previously seen keys", len(seen_keys))

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        soup, viewstate, form_id, table_id = _initial_fetch(session)
    except requests.RequestException as exc:
        logger.error("Failed to fetch HHS portal: %s", exc)
        raise ConnectionError(f"HHS OCR portal unreachable: {PORTAL_URL}") from exc

    rows_per_page = _detect_rows_per_page(soup)
    logger.info("rows_per_page=%d", rows_per_page)

    # Parse page 1 from the initial GET response
    all_records: list[dict] = _parse_table_html(soup)
    if not all_records:
        all_records = _parse_results_text(soup)
    logger.info("Page 1: %d records", len(all_records))

    # Check early termination: if every record on this page predates cutoff
    def _all_old(recs: list[dict]) -> bool:
        if not recs:
            return False
        dated = [r for r in recs if r.get("date_str")]
        if not dated:
            return False
        return all(
            try_parse_date(r["date_str"]) is not None
            and try_parse_date(r["date_str"]) < CUTOFF_DATE
            for r in dated
        )

    if not table_id:
        logger.warning("table_id not detected; skipping AJAX pagination")
    else:
        for page_num in range(2, MAX_PAGES + 1):
            if _all_old(all_records[-rows_per_page:]):
                logger.info("All recent records are before cutoff – stopping early")
                break

            first_row = (page_num - 1) * rows_per_page
            logger.info("Fetching AJAX page %d (first_row=%d)", page_num, first_row)

            try:
                page_records, new_vs = _ajax_next_page(
                    session, viewstate, form_id, table_id, first_row, rows_per_page
                )
            except requests.RequestException as exc:
                logger.error("AJAX request failed on page %d: %s", page_num, exc)
                break

            if not page_records:
                logger.info("No records on page %d – reached end of results", page_num)
                break

            if new_vs:
                viewstate = new_vs

            all_records.extend(page_records)
            logger.info("Page %d: %d records (total so far: %d)",
                        page_num, len(page_records), len(all_records))
            time.sleep(1)

    logger.info("Total raw records: %d", len(all_records))

    new_records, updated_seen = filter_new(all_records, seen_keys)
    save_seen_keys(updated_seen)

    logger.info("=== HHS scrape complete: %d new records ===", len(new_records))
    return new_records


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    results = scrape()
    print(f"\nNew HHS OCR breach notifications found: {len(results)}")
    for r in results:
        label = f"[{r['date_str']}]" if r["date_str"] else "[no date]"
        print(f"  {label}  {r['entity']}  ({r['state']})")
        print(f"           individuals: {r['individuals_affected']}  "
              f"type: {r['breach_type']}")
