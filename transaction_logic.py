import csv
import json
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "transaction_id", "transaction_date", "customer_id", "customer_segment",
    "product_id", "product_name", "category", "subcategory", "quantity",
    "unit_price", "discount_pct", "tax_pct", "shipping_cost",
    "payment_method", "warehouse_id", "delivery_days", "returned",
]

def validate_schema(csv_path: str, **context) -> str:
    logger.info(f"Checking schema for {csv_path}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        missing = [c for c in REQUIRED_COLUMNS if c not in headers]
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        null_ids = []
        for i, row in enumerate(reader, start=2): 
            if not row.get("transaction_id", "").strip():
                null_ids.append(i)

        if null_ids:
            raise ValueError(f"Found blank transaction IDs on lines: {null_ids}")

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="validated_csv_path", value=csv_path)

    return csv_path

def compute_net_revenue(csv_path: str, output_path: str, **context) -> str:
    enriched_rows = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = (reader.fieldnames or []) + ["net_revenue"]

        for row in reader:
            qty = float(row["quantity"])
            price = float(row["unit_price"])
            disc = float(row["discount_pct"])
            tax = float(row["tax_pct"])
            ship = float(row["shipping_cost"])

            net_rev = (qty * price * (1 - disc / 100) * (1 + tax / 100) + ship)
            row["net_revenue"] = round(net_rev, 2)
            enriched_rows.append(row)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_rows)

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="enriched_csv_path", value=output_path)

    return output_path

def compute_category_summary(enriched_csv_path: str, output_path: str, **context) -> str:
    revenue_by_cat = defaultdict(float)
    count_by_cat = defaultdict(int)

    with open(enriched_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            c = row["category"]
            revenue_by_cat[c] += float(row["net_revenue"])
            count_by_cat[c] += 1

    summary = {
        c: {
            "total_net_revenue": round(revenue_by_cat[c], 2),
            "transaction_count": count_by_cat[c],
        }
        for c in sorted(revenue_by_cat)
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="category_summary_path", value=output_path)

    return output_path

def compute_return_rate(csv_path: str, output_path: str, **context) -> str:
    total_by_cat = defaultdict(int)
    returned_by_cat = defaultdict(int)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            c = row["category"]
            total_by_cat[c] += 1
            if row["returned"].strip().lower() == "yes":
                returned_by_cat[c] += 1

    rates = {
        c: {
            "total_orders": total_by_cat[c],
            "returned_orders": returned_by_cat[c],
            "return_rate_pct": round((returned_by_cat[c] / total_by_cat[c]) * 100, 2),
        }
        for c in sorted(total_by_cat)
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rates, f, indent=2)

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="return_rate_path", value=output_path)

    return output_path

def update_run_log(csv_path: str, **context) -> None:
    ti = context["ti"]
    execution_ds = context.get("ds", "unknown-date")

    cat_path = ti.xcom_pull(task_ids="transform.compute_category_summary", key="category_summary_path")
    ret_path = ti.xcom_pull(task_ids="transform.compute_return_rate", key="return_rate_path")

    with open(csv_path, newline="", encoding="utf-8") as f:
        total_records = sum(1 for _ in csv.DictReader(f))

    with open(cat_path, encoding="utf-8") as f:
        category_summary = json.load(f)

    with open(ret_path, encoding="utf-8") as f:
        return_rates = json.load(f)

    total_revenue = sum(v["total_net_revenue"] for v in category_summary.values())

    logger.info("=" * 50)
    logger.info(f"RUN SUMMARY FOR: {execution_ds}")
    logger.info(f"Total Rows: {total_records}")
    logger.info(f"Gross Revenue: INR {total_revenue:.2f}")
    logger.info("-" * 50)
    
    for cat, stats in sorted(category_summary.items(), key=lambda x: -x[1]["total_net_revenue"]):
        rr = return_rates.get(cat, {}).get("return_rate_pct", 0.0)
        logger.info(f"{cat}: INR {stats['total_net_revenue']} | Returns: {rr}%")
    logger.info("=" * 50)