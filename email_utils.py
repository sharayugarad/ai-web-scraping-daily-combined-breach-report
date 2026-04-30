"""
Email utility for Daily Combined Breach Report.

Loads SMTP credentials from a .env file and sends an HTML email
summarising new breach notices from the active data sources.

Usage (from run_daily.py):
    from email_utils import send_report
    send_report(env_path="/path/to/secret.local.env",
                nh_records=[...], hhs_records=[...])
"""

import logging
import os
import smtplib
import ssl
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import dotenv_values

logger = logging.getLogger("email_utils")


# ─── Config loader ────────────────────────────────────────────────────────────

def _load_config(env_path: str) -> dict:
    """Load SMTP settings from a .env file."""
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env file not found: {env_path}")
    cfg = dotenv_values(env_path)
    required = ["SMTP_SERVER", "SMTP_PORT", "SENDER_EMAIL",
                "SENDER_PASSWORD", "RECEIVER_EMAILS"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise ValueError(f"Missing required keys in {env_path}: {missing}")
    return cfg


# ─── HTML builder ─────────────────────────────────────────────────────────────

def _build_html(nh_records: list[dict], hhs_records: list[dict],
                run_ts: str, nh_unavailable: bool = False,
                hhs_unavailable: bool = False) -> str:
    """Return an HTML email body."""

    def _records_table(records: list[dict]) -> str:
        if not records:
            return "<p style='color:#555;'>No new records found.</p>"
        rows = ""
        for r in records:
            date_label = r.get("date_str") or r.get("date_iso") or "—"
            entity = r.get("entity", "Unknown")
            url = r.get("url", "")
            link = (f'<a href="{url}" style="color:#1a73e8;">{entity}</a>'
                    if url else entity)
            rows += (
                f"<tr>"
                f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;'>{link}</td>"
                f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;white-space:nowrap;'>{date_label}</td>"
                f"</tr>"
            )
        return (
            "<table style='border-collapse:collapse;width:100%;font-size:14px;'>"
            "<thead>"
            "<tr style='background:#f5f5f5;'>"
            "<th style='padding:8px 12px;text-align:left;'>Entity</th>"
            "<th style='padding:8px 12px;text-align:left;'>Date</th>"
            "</tr>"
            "</thead>"
            f"<tbody>{rows}</tbody>"
            "</table>"
        )

    def _hhs_table(records: list[dict]) -> str:
        if not records:
            return "<p style='color:#555;'>No new records found.</p>"
        rows = ""
        for r in records:
            date_label = r.get("date_str") or r.get("date_iso") or "—"
            entity = r.get("entity", "Unknown")
            state = r.get("state", "—")
            individuals = r.get("individuals_affected", "—")
            breach_type = r.get("breach_type", "—")
            url = r.get("url", "")
            link = (f'<a href="{url}" style="color:#1a73e8;">{entity}</a>'
                    if url else entity)
            rows += (
                f"<tr>"
                f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;'>{link}</td>"
                f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;white-space:nowrap;'>{state}</td>"
                f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;white-space:nowrap;'>{individuals}</td>"
                f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;white-space:nowrap;'>{date_label}</td>"
                f"<td style='padding:6px 12px;border-bottom:1px solid #e0e0e0;'>{breach_type}</td>"
                f"</tr>"
            )
        return (
            "<table style='border-collapse:collapse;width:100%;font-size:14px;'>"
            "<thead>"
            "<tr style='background:#f5f5f5;'>"
            "<th style='padding:8px 12px;text-align:left;'>Entity</th>"
            "<th style='padding:8px 12px;text-align:left;'>State</th>"
            "<th style='padding:8px 12px;text-align:left;'>Individuals Affected</th>"
            "<th style='padding:8px 12px;text-align:left;'>Date</th>"
            "<th style='padding:8px 12px;text-align:left;'>Type of Breach</th>"
            "</tr>"
            "</thead>"
            f"<tbody>{rows}</tbody>"
            "</table>"
        )

    nh_count  = len(nh_records)
    hhs_count = len(hhs_records)
    total = nh_count + hhs_count

    subject_line = (
        f"{total} new breach notice{'s' if total != 1 else ''} "
        f"({nh_count} NH · {hhs_count} HHS)"
    )

    nh_section_html = (
        "<p style='color:#e65100;font-size:13px;'>"
        "&#9888;&nbsp;Source unavailable - fetch failed.</p>"
        if nh_unavailable
        else _records_table(nh_records)
    )
    hhs_section_html = (
        "<p style='color:#e65100;font-size:13px;'>"
        "&#9888;&nbsp;Source unavailable - fetch failed.</p>"
        if hhs_unavailable
        else _hhs_table(hhs_records)
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Daily Combined Breach Report</title></head>
<body style="font-family:Arial,sans-serif;color:#333;max-width:900px;margin:0 auto;padding:20px;">
  <h2 style="border-bottom:2px solid #d32f2f;padding-bottom:8px;color:#d32f2f;">
    Daily Combined Breach Report
  </h2>
  <p style="color:#777;font-size:13px;">Generated: {run_ts} UTC</p>
  <p><strong>{subject_line}</strong></p>

  <h3 style="margin-top:28px;">New Hampshire DOJ
    <span style="font-weight:normal;font-size:14px;color:#555;">
      ({nh_count} new)
    </span>
  </h3>
  {nh_section_html}

  <h3 style="margin-top:28px;">HHS OCR – HIPAA Breach Report
    <span style="font-weight:normal;font-size:14px;color:#555;">
      ({hhs_count} new)
    </span>
  </h3>
  {hhs_section_html}

  <hr style="margin-top:32px;border:none;border-top:1px solid #e0e0e0;">
  <p style="font-size:12px;color:#999;">
    Sources:
    <a href="https://doj.nh.gov/citizens/consumer-protection-antitrust-bureau/security-breach-notifications"
       style="color:#999;">NH DOJ</a> ·
    <a href="https://ocrportal.hhs.gov/ocr/breach/breach_report_hip.jsf"
       style="color:#999;">HHS OCR</a>
  </p>
</body>
</html>"""

def _build_subject(nh_records: list[dict],
                   hhs_records: list[dict]) -> str:  # noqa: ARG001
    return "🔑 Daily Combined Breach Report"


# ─── SMTP sender ──────────────────────────────────────────────────────────────

def send_report(env_path: str,
                nh_records: list[dict],
                hhs_records: list[dict],
                nh_unavailable: bool = False,
                hhs_unavailable: bool = False) -> None:
    """
    Build and send the breach-report email.

    Args:
        env_path:    Absolute path to the .env file with SMTP credentials.
        nh_records:  New NH breach records (list of dicts from scraper_nh.scrape()).
        hhs_records: New HHS breach records (list of dicts from scraper_hhs.scrape()).
        nh_unavailable: True when the NH site was unreachable.
        hhs_unavailable: True when the HHS portal was unreachable.
    """
    cfg = _load_config(env_path)

    smtp_server  = cfg["SMTP_SERVER"]
    smtp_port    = int(cfg["SMTP_PORT"])
    use_ssl      = cfg.get("USE_SSL", "false").strip().lower() in ("true", "1", "yes")
    sender       = cfg["SENDER_EMAIL"]
    password     = cfg["SENDER_PASSWORD"]
    receivers_raw = cfg["RECEIVER_EMAILS"]
    receivers    = [e.strip() for e in receivers_raw.split(",") if e.strip()]

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = _build_subject(nh_records, hhs_records)
    msg["From"]    = sender
    msg["To"]      = ", ".join(receivers)

    html_body = _build_html(
        nh_records,
        hhs_records,
        run_ts,
        nh_unavailable=nh_unavailable,
        hhs_unavailable=hhs_unavailable,
    )
    # Plain-text fallback
    total = len(nh_records) + len(hhs_records)
    plain = (
        f"Daily Combined Breach Report – {run_ts} UTC\n\n"
        f"Total new notices: {total}\n"
        f"  NH DOJ  : {len(nh_records)}\n"
        f"  HHS OCR : {len(hhs_records)}\n\n"
    )
    for label, recs in [
            ("New Hampshire DOJ", nh_records),
            ("HHS OCR – HIPAA",   hhs_records)]:
        plain += f"--- {label} ---\n"
        if label == "New Hampshire DOJ" and nh_unavailable:
            plain += "  Source unavailable - fetch failed.\n"
        elif label == "HHS OCR – HIPAA" and hhs_unavailable:
            plain += "  Source unavailable - fetch failed.\n"
        elif recs:
            for r in recs:
                d = r.get("date_str") or r.get("date_iso") or "no date"
                plain += f"  [{d}] {r.get('entity','?')}  {r.get('url','')}\n"
        else:
            plain += "  No new records.\n"
        plain += "\n"

    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    logger.info("Sending report to %s via %s:%d (SSL=%s)",
                receivers, smtp_server, smtp_port, use_ssl)
    try:
        if use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
                server.login(sender, password)
                server.sendmail(sender, receivers, msg.as_string())
        else:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
                server.login(sender, password)
                server.sendmail(sender, receivers, msg.as_string())
        logger.info("Email sent successfully.")
    except smtplib.SMTPException as exc:
        logger.error("Failed to send email: %s", exc)
        raise
