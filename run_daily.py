"""
Daily Combined Breach Report – Main Orchestrator

Runs the active scrapers and sends an email summary of new breach notices.

Usage:
    python run_daily.py

Schedule (cron example – runs at 07:00 every day):
    0 7 * * * /usr/bin/python3 /path/to/run_daily.py >> /path/to/run_daily.log 2>&1
"""

import logging
import sys
from datetime import datetime, timezone

import scraper_nh
import scraper_hhs
from email_utils import send_report

# ─── Configuration ────────────────────────────────────────────────────────────

# Absolute path to the .env file containing SMTP credentials.
# Edit this before running.
ENV_PATH = '/Users/sharayu/CodeLab/secrets.local.env'

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("run_daily")


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    start = datetime.now(timezone.utc)
    logger.info("=== Daily Breach Report run started at %s UTC ===",
                start.strftime("%Y-%m-%d %H:%M:%S"))

    # Run scrapers
    nh_unavailable = False
    logger.info("--- Running NH DOJ scraper ---")
    try:
        nh_records = scraper_nh.scrape()
    except ConnectionError as exc:
        logger.warning("NH scraper unavailable: %s", exc)
        nh_records = []
        nh_unavailable = True
    except Exception as exc:
        logger.error("NH scraper failed: %s", exc, exc_info=True)
        nh_records = []

    hhs_unavailable = False
    logger.info("--- Running HHS OCR scraper ---")
    try:
        hhs_records = scraper_hhs.scrape()
    except ConnectionError as exc:
        logger.warning("HHS scraper unavailable: %s", exc)
        hhs_records = []
        hhs_unavailable = True
    except Exception as exc:
        logger.error("HHS scraper failed: %s", exc, exc_info=True)
        hhs_records = []

    total = len(nh_records) + len(hhs_records)
    logger.info("Scrape complete – NH: %d new, HHS: %d new, total: %d",
                len(nh_records), len(hhs_records), total)

    # Send email
    logger.info("--- Sending email report ---")
    try:
        send_report(
            env_path=ENV_PATH,
            nh_records=nh_records,
            hhs_records=hhs_records,
            nh_unavailable=nh_unavailable,
            hhs_unavailable=hhs_unavailable,
        )
    except FileNotFoundError as exc:
        logger.error("Could not load .env file – email not sent: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Email send failed: %s", exc, exc_info=True)
        sys.exit(1)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info("=== Run complete in %.1fs – %d new notice%s ===",
                elapsed, total, "s" if total != 1 else "")


if __name__ == "__main__":
    main()
