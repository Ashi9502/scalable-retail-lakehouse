from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.config import load_config, project_path


CITIES = np.array(
    ["Bengaluru", "Delhi", "Mumbai", "Hyderabad", "Pune", "Chennai", "Kolkata", "Jaipur"]
)
CATEGORIES = np.array(
    ["Electronics", "Home", "Beauty", "Books", "Fitness", "Fashion", "Grocery"]
)
STATUSES = np.array(["CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"])
PAYMENT_METHODS = np.array(["UPI", "CARD", "NET_BANKING", "COD"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic retail datasets.")
    parser.add_argument("--orders", type=int, default=500_000)
    parser.add_argument("--customers", type=int, default=50_000)
    parser.add_argument("--products", type=int, default=5_000)
    parser.add_argument("--chunk-size", type=int, default=250_000)
    parser.add_argument("--inject-bad-ratio", type=float, default=0.005)
    return parser.parse_args()


def generate_customers(rng: np.random.Generator, count: int, out: Path) -> None:
    ids = np.arange(1, count + 1, dtype=np.int64)
    df = pd.DataFrame(
        {
            "customer_id": ids,
            "customer_name": [f"customer_{i:07d}" for i in ids],
            "city": rng.choice(CITIES, count),
            "signup_date": pd.Timestamp("2022-01-01")
            + pd.to_timedelta(rng.integers(0, 1500, count), unit="D"),
            "updated_at": pd.Timestamp("2026-01-01")
            + pd.to_timedelta(rng.integers(0, 270, count), unit="D"),
        }
    )
    df.to_csv(out / "customers.csv", index=False)


def generate_products(rng: np.random.Generator, count: int, out: Path) -> None:
    ids = np.arange(1, count + 1, dtype=np.int64)
    base_prices = np.round(rng.lognormal(mean=5.0, sigma=1.0, size=count), 2)
    df = pd.DataFrame(
        {
            "product_id": ids,
            "product_name": [f"product_{i:06d}" for i in ids],
            "category": rng.choice(CATEGORIES, count),
            "unit_price": np.maximum(base_prices, 10),
            "updated_at": pd.Timestamp("2026-01-01")
            + pd.to_timedelta(rng.integers(0, 270, count), unit="D"),
        }
    )
    df.to_csv(out / "products.csv", index=False)


def generate_orders(
    rng: np.random.Generator,
    total: int,
    customers: int,
    products: int,
    chunk_size: int,
    bad_ratio: float,
    out: Path,
    start_date: str,
    end_date: str,
) -> None:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    days = (end - start).days + 1

    first = True
    for offset in range(0, total, chunk_size):
        size = min(chunk_size, total - offset)
        order_ids = np.arange(offset + 1, offset + size + 1, dtype=np.int64)
        quantity = rng.integers(1, 6, size)
        unit_price = np.round(rng.lognormal(mean=5.1, sigma=1.0, size=size), 2)
        discount = np.round(rng.uniform(0, 0.30, size), 4)
        amount = np.round(quantity * unit_price * (1 - discount), 2)

        df = pd.DataFrame(
            {
                "order_id": order_ids,
                "customer_id": rng.integers(1, customers + 1, size),
                "product_id": rng.integers(1, products + 1, size),
                "order_ts": start
                + pd.to_timedelta(rng.integers(0, days * 86400, size), unit="s"),
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_pct": discount,
                "order_amount": amount,
                "status": rng.choice(
                    STATUSES, size, p=[0.05, 0.12, 0.13, 0.63, 0.07]
                ),
                "payment_method": rng.choice(PAYMENT_METHODS, size),
                "updated_at": pd.Timestamp("2026-01-01")
                + pd.to_timedelta(rng.integers(0, 270 * 86400, size), unit="s"),
            }
        )

        bad_count = int(size * bad_ratio)
        if bad_count > 0:
            bad_idx = rng.choice(size, bad_count, replace=False)
            half = max(1, bad_count // 2)
            df.loc[bad_idx[:half], "order_amount"] = -1.0
            df.loc[bad_idx[half:], "customer_id"] = customers + 999_999

        df.to_csv(
            out / "orders.csv",
            mode="w" if first else "a",
            header=first,
            index=False,
        )
        first = False
        print(f"generated {offset + size:,}/{total:,} orders")


def main() -> None:
    args = parse_args()
    cfg = load_config()
    raw = project_path(cfg["paths"]["raw"])
    raw.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(cfg["project"].get("random_seed", 42))

    generate_customers(rng, args.customers, raw)
    generate_products(rng, args.products, raw)
    generate_orders(
        rng,
        args.orders,
        args.customers,
        args.products,
        args.chunk_size,
        args.inject_bad_ratio,
        raw,
        cfg["generator"]["start_date"],
        cfg["generator"]["end_date"],
    )
    print(f"raw data written to {raw}")


if __name__ == "__main__":
    main()
