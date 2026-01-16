import json
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from src.config import REPORTS_DIR
from src.common.logger import get_logger

logger = get_logger("dq_pdf")

def load_latest_report(pattern: str) -> dict:
    files = sorted(REPORTS_DIR.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No reports found matching: {pattern}")
    latest = files[-1]
    return json.loads(latest.read_text(encoding="utf-8")), latest.name

def write_section(c, title, y, lines):
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, title)
    y -= 18
    c.setFont("Helvetica", 10)
    for line in lines:
        if y < 60:
            c.showPage()
            y = 800
            c.setFont("Helvetica", 10)
        c.drawString(60, y, line[:120])
        y -= 14
    return y

def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    interactions, i_name = load_latest_report("validation_interactions_*.json")
    products, p_name = load_latest_report("validation_products_*.json")

    out_pdf = REPORTS_DIR / "Data_Quality_Report.pdf"
    c = canvas.Canvas(str(out_pdf), pagesize=A4)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, 800, "Data Quality Report (Task 4)")

    y = 770
    y = write_section(c, "Inputs", y, [
        f"Interactions report: {i_name}",
        f"Products report: {p_name}",
    ])

    # Interactions summary
    y = write_section(c, "Interactions - Summary", y, [
        f"Partition: {interactions.get('partition')}",
        f"Rows: {interactions.get('row_count')}",
        f"Schema issues: {len(interactions.get('schema_issues', []))}",
        f"Missing values: {sum(interactions.get('missing_values', {}).values())}",
        f"Duplicate rows (keyed): {interactions.get('duplicate_rows_on_key', {}).get('count')}",
        f"Bad timestamps: {interactions.get('format_checks', {}).get('bad_timestamps')}",
        f"Bad event types: {interactions.get('range_checks', {}).get('bad_event_types')}",
        f"Price out of range: {interactions.get('range_checks', {}).get('price_out_of_range')}",
    ])

    if interactions.get("schema_issues"):
        y = write_section(c, "Interactions - Schema Issues", y, interactions["schema_issues"])

    # Products summary
    y = write_section(c, "Products - Summary", y, [
        f"Partition: {products.get('partition')}",
        f"Rows: {products.get('row_count')}",
        f"Schema issues: {len(products.get('schema_issues', []))}",
        f"Missing values: {sum(products.get('missing_values', {}).values())}",
        f"Duplicate product IDs: {products.get('duplicate_rows_on_key', {}).get('count')}",
    ])

    if products.get("schema_issues"):
        y = write_section(c, "Products - Schema Issues", y, products["schema_issues"])

    c.save()
    logger.info(f"Wrote PDF report: {out_pdf}")

if __name__ == "__main__":
    main()
