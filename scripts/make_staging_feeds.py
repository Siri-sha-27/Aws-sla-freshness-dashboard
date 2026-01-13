from pathlib import Path
import pandas as pd
from datetime import datetime, timezone, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

ORDERS_IN = RAW_DIR / "orders" / "olist_orders_dataset.csv"
PAYMENTS_IN = RAW_DIR / "payments" / "olist_order_payments_dataset.csv"
PRODUCTS_IN = RAW_DIR / "products" / "olist_products_dataset.csv"

OUT_BASE = RAW_DIR / "staging_feeds"  # upload this to S3 under staging/


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def make_orders_last_7_days():
    df = pd.read_csv(ORDERS_IN)
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["order_purchase_timestamp"])

    # take last 7 unique dates from dataset (UTC dates)
    df["orig_date"] = df["order_purchase_timestamp"].dt.date.astype(str)
    last_dates = sorted(df["orig_date"].unique())[-7:]
    df7 = df[df["orig_date"].isin(last_dates)].copy()

    # map these 7 dates to "today-6 ... today" so SLA testing matches now
    today = datetime.now(timezone.utc).date()
    mapped_dates = [(today - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]
    mapping = dict(zip(last_dates, mapped_dates))

    df7["feed_date"] = df7["orig_date"].map(mapping)

    for date_str, g in df7.groupby("feed_date"):
        out_dir = OUT_BASE / "orders" / date_str
        ensure_dir(out_dir)
        out_file = out_dir / f"orders_{date_str}.csv"
        g.drop(columns=["feed_date"]).to_csv(out_file, index=False)

    print(f"[OK] Orders staging feed created at: {OUT_BASE / 'orders'}")


def make_payments_last_24_hours():
    payments = pd.read_csv(PAYMENTS_IN)
    orders = pd.read_csv(ORDERS_IN, usecols=["order_id", "order_purchase_timestamp"])

    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce", utc=True)
    merged = payments.merge(orders, on="order_id", how="left").dropna(subset=["order_purchase_timestamp"])

    # use the same 7-day mapping as orders: map dataset's last date to "today"
    merged["orig_date"] = merged["order_purchase_timestamp"].dt.date.astype(str)
    last_dates = sorted(merged["orig_date"].unique())[-7:]
    today = datetime.now(timezone.utc).date()
    mapped_dates = [(today - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]
    mapping = dict(zip(last_dates, mapped_dates))

    merged["feed_date"] = merged["orig_date"].map(mapping)
    merged = merged.dropna(subset=["feed_date"])

    # assign "feed_time" by keeping original hour but moving to mapped feed_date
    merged["hour"] = merged["order_purchase_timestamp"].dt.hour.astype(int)

    # keep only the last 24 feed-hours (based on mapped dates/hours)
    merged["feed_dt"] = (
        pd.to_datetime(merged["feed_date"], utc=True) +
        pd.to_timedelta(merged["hour"], unit="h")
    )

    cutoff = pd.Timestamp(datetime.now(timezone.utc) - timedelta(hours=24))
    merged = merged[merged["feed_dt"] >= cutoff.floor("h")]


    for (date_str, hour), g in merged.groupby(["feed_date", "hour"]):
        out_dir = OUT_BASE / "payments" / date_str / f"hour={hour:02d}"
        ensure_dir(out_dir)
        out_file = out_dir / f"payments_{date_str}_h{hour:02d}.csv"
        g.to_csv(out_file, index=False)

    print(f"[OK] Payments staging feed created at: {OUT_BASE / 'payments'}")


def make_products_snapshot():
    df = pd.read_csv(PRODUCTS_IN)
    out_dir = OUT_BASE / "products" / "snapshot"
    ensure_dir(out_dir)
    out_file = out_dir / "products_snapshot.csv"
    df.to_csv(out_file, index=False)
    print(f"[OK] Products snapshot created at: {out_file}")


if __name__ == "__main__":
    ensure_dir(OUT_BASE)
    make_orders_last_7_days()
    make_payments_last_24_hours()
    make_products_snapshot()
    print("\nDone ✅ Upload data/raw/staging_feeds/ to S3 under staging/")
