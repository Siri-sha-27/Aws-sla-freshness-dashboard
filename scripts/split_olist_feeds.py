import os
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

ORDERS_IN = RAW_DIR / "orders" / "olist_orders_dataset.csv"
PAYMENTS_IN = RAW_DIR / "payments" / "olist_order_payments_dataset.csv"
PRODUCTS_IN = RAW_DIR / "products" / "olist_products_dataset.csv"

OUT_BASE = RAW_DIR / "feeds"   # we will upload this folder to S3


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def split_orders_daily():
    df = pd.read_csv(ORDERS_IN)
    # Olist orders has this column (you confirmed it):
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")

    df = df.dropna(subset=["order_purchase_timestamp"])
    df["date"] = df["order_purchase_timestamp"].dt.date.astype(str)

    for date_str, g in df.groupby("date"):
        out_dir = OUT_BASE / "orders" / date_str
        ensure_dir(out_dir)
        out_file = out_dir / f"orders_{date_str}.csv"
        g.drop(columns=["date"]).to_csv(out_file, index=False)

    print(f"[OK] Orders split into daily files at: {OUT_BASE / 'orders'}")


def split_payments_hourly():
    # payments file doesn't include timestamp by default, so we JOIN with orders to get order_purchase_timestamp
    payments = pd.read_csv(PAYMENTS_IN)
    orders = pd.read_csv(ORDERS_IN, usecols=["order_id", "order_purchase_timestamp"])

    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")
    merged = payments.merge(orders, on="order_id", how="left").dropna(subset=["order_purchase_timestamp"])

    merged["date"] = merged["order_purchase_timestamp"].dt.date.astype(str)
    merged["hour"] = merged["order_purchase_timestamp"].dt.hour.astype(int)

    for (date_str, hour), g in merged.groupby(["date", "hour"]):
        out_dir = OUT_BASE / "payments" / date_str / f"hour={hour:02d}"
        ensure_dir(out_dir)
        out_file = out_dir / f"payments_{date_str}_h{hour:02d}.csv"
        g.to_csv(out_file, index=False)

    print(f"[OK] Payments split into hourly files at: {OUT_BASE / 'payments'}")


def prepare_products_snapshot():
    # products doesn't have time; treat as weekly snapshot (single file)
    df = pd.read_csv(PRODUCTS_IN)
    out_dir = OUT_BASE / "products" / "snapshot"
    ensure_dir(out_dir)
    out_file = out_dir / "products_snapshot.csv"
    df.to_csv(out_file, index=False)
    print(f"[OK] Products snapshot saved at: {out_file}")


if __name__ == "__main__":
    ensure_dir(OUT_BASE)
    split_orders_daily()
    split_payments_hourly()
    prepare_products_snapshot()
    print("\nDone ✅ Next: we will upload data/raw/feeds/ to S3.")
