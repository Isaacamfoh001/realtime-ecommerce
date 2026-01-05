import csv
import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from faker import Faker

@dataclass
class GeneratorConfig:
    out_dir: Path
    rows_per_file: int = 25
    interval_seconds: float = 2.0
    total_files: Optional[int] = None  # None = run forever
    seed: int = 42
    bad_row_rate: float = 0.01  # 1% messy rows (optional, helps test cleaning)


def utc_now_iso() -> str:
    # Example: 2026-01-05T10:15:30.123456+00:00
    return datetime.now(timezone.utc).isoformat()


def build_product_catalog() -> List[Dict]:
    # Small, realistic product catalog (you can expand later)
    return [
        {"product_id": 1001, "product_name": "Rice (5kg)", "category": "Groceries", "price_min": 65.00, "price_max": 95.00},
        {"product_id": 1002, "product_name": "Cooking Oil (1L)", "category": "Groceries", "price_min": 25.00, "price_max": 45.00},
        {"product_id": 1003, "product_name": "Phone Charger (USB-C)", "category": "Electronics", "price_min": 35.00, "price_max": 80.00},
        {"product_id": 1004, "product_name": "Bluetooth Earbuds", "category": "Electronics", "price_min": 120.00, "price_max": 250.00},
        {"product_id": 1005, "product_name": "Bath Soap (Pack)", "category": "Home & Care", "price_min": 10.00, "price_max": 30.00},
        {"product_id": 1006, "product_name": "Detergent (2kg)", "category": "Home & Care", "price_min": 40.00, "price_max": 85.00},
        {"product_id": 1007, "product_name": "Sneakers", "category": "Fashion", "price_min": 180.00, "price_max": 420.00},
        {"product_id": 1008, "product_name": "T-Shirt", "category": "Fashion", "price_min": 50.00, "price_max": 130.00},
    ]


def maybe_make_bad_row(row: Dict, rng: random.Random, bad_row_rate: float) -> Dict:
    """
    Introduce a tiny % of messy rows so your Spark job can demonstrate cleaning.
    Keep it small so it doesn't break everything.
    """
    if rng.random() > bad_row_rate:
        return row

    # Choose one kind of "badness"
    choice = rng.choice(["bad_action", "missing_price", "bad_timestamp"])
    bad = dict(row)

    if choice == "bad_action":
        bad["action"] = "click"  # invalid (not view/purchase)
    elif choice == "missing_price":
        bad["price"] = ""  # empty
    elif choice == "bad_timestamp":
        bad["event_time"] = "not_a_timestamp"

    return bad


def generate_event(fake: Faker, rng: random.Random, catalog: List[Dict]) -> Dict:
    product = rng.choice(catalog)
    action = rng.choices(["view", "purchase"], weights=[0.8, 0.2], k=1)[0]

    base = {
        "event_id": str(uuid.uuid4()),
        "event_time": utc_now_iso(),
        "user_id": rng.randint(1, 5000),
        "action": action,
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "price": "",
        "quantity": "",
        "source": rng.choice(["web", "mobile"]),
    }

    if action == "purchase":
        price = rng.uniform(product["price_min"], product["price_max"])
        quantity = rng.randint(1, 5)
        base["price"] = f"{price:.2f}"
        base["quantity"] = str(quantity)

    return base


def write_csv_atomic(out_path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    """
    Writes to a temp file first, then renames it.
    This avoids Spark reading a half-written CSV file.
    """
    tmp_path = out_path.with_suffix(".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    tmp_path.replace(out_path)  # atomic rename on most filesystems


def main() -> None:
    # Config via env vars (simple & practical)
    out_dir = Path(os.getenv("OUT_DIR", "data/incoming"))
    rows_per_file = int(os.getenv("ROWS_PER_FILE", "25"))
    interval_seconds = float(os.getenv("INTERVAL_SECONDS", "2"))
    total_files_raw = os.getenv("TOTAL_FILES", "")
    total_files = int(total_files_raw) if total_files_raw.strip() else None
    seed = int(os.getenv("SEED", "42"))
    bad_row_rate = float(os.getenv("BAD_ROW_RATE", "0.01"))

    cfg = GeneratorConfig(
        out_dir=out_dir,
        rows_per_file=rows_per_file,
        interval_seconds=interval_seconds,
        total_files=total_files,
        seed=seed,
        bad_row_rate=bad_row_rate,
    )

    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    fake = Faker()
    rng = random.Random(cfg.seed)
    catalog = build_product_catalog()

    fieldnames = [
        "event_id",
        "event_time",
        "user_id",
        "action",
        "product_id",
        "product_name",
        "category",
        "price",
        "quantity",
        "source",
    ]

    print("=== Real-time E-commerce Event Generator ===")
    print(f"Output folder: {cfg.out_dir.resolve()}")
    print(f"Rows per file: {cfg.rows_per_file}")
    print(f"Interval (s):  {cfg.interval_seconds}")
    print(f"Total files:   {'∞ (until stopped)' if cfg.total_files is None else cfg.total_files}")
    print(f"Bad row rate:  {cfg.bad_row_rate * 100:.2f}%")
    print("Press Ctrl+C to stop.\n")

    file_count = 0
    try:
        while cfg.total_files is None or file_count < cfg.total_files:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
            out_path = cfg.out_dir / f"events_{timestamp}.csv"

            rows = []
            for _ in range(cfg.rows_per_file):
                row = generate_event(fake, rng, catalog)
                row = maybe_make_bad_row(row, rng, cfg.bad_row_rate)
                rows.append(row)

            write_csv_atomic(out_path, rows, fieldnames)
            file_count += 1

            print(f"[{file_count}] wrote {len(rows)} events -> {out_path.name}")
            time.sleep(cfg.interval_seconds)

    except KeyboardInterrupt:
        print("\nStopped generator.")


if __name__ == "__main__":
    main()
