"""Bootstrap local infrastructure with buckets and seed data.

Run via `make seed` after `make up`. Idempotent.
"""

from __future__ import annotations

import csv
import io
import os
import random
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.client import Config

from src.utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_DIR = REPO_ROOT / "data" / "sample"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        log.info("bucket_exists", bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)
        log.info("bucket_created", bucket=bucket)


def generate_synthetic_transactions(n_rows: int = 5000) -> str:
    """Produce a synthetic transactions CSV for reproducibility in CI."""
    rng = random.Random(42)
    start = date.today() - timedelta(days=180)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "transaction_id", "customer_id", "transaction_date", "transaction_ts",
        "amount", "currency", "merchant_id", "merchant_category", "country",
        "channel", "is_fraud", "email", "ip_address",
    ])
    categories = ["GROCERY", "TRAVEL", "ELECTRONICS", "RESTAURANT", "ENERGY", "STREAMING"]
    countries = ["US", "GB", "DE", "FR", "JP", "BR", "IN"]
    channels = ["WEB", "MOBILE", "POS"]
    for i in range(n_rows):
        d = start + timedelta(days=rng.randint(0, 180))
        writer.writerow([
            f"T{i:08d}",
            f"C{rng.randint(1, 1500):06d}",
            d.isoformat(),
            f"{d.isoformat()}T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}Z",
            round(rng.uniform(1.99, 1999.99), 2),
            rng.choice(["USD", "EUR", "GBP", "JPY"]),
            f"M{rng.randint(1, 800):05d}",
            rng.choice(categories),
            rng.choice(countries),
            rng.choice(channels),
            int(rng.random() < 0.012),
            f"customer{rng.randint(1, 1500)}@example.com",
            f"203.0.113.{rng.randint(1, 254)}",
        ])
    return buf.getvalue()


def main() -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Buckets
    s3 = _s3_client()
    for bucket in [
        os.getenv("AWS_S3_BUCKET_BRONZE", "finsight-bronze"),
        os.getenv("AWS_S3_BUCKET_SILVER", "finsight-silver"),
    ]:
        ensure_bucket(s3, bucket)

    # 2. Sample synthetic transactions (committed sample is small; this is full)
    csv_path = SAMPLE_DIR / "synthetic_transactions_full.csv"
    if not csv_path.exists():
        csv_path.write_text(generate_synthetic_transactions(), encoding="utf-8")
        log.info("synthetic_transactions_generated", path=str(csv_path))

    # 3. Reference assets
    ref_path = SAMPLE_DIR / "reference_assets.csv"
    if not ref_path.exists():
        ref_path.write_text(
            "asset_symbol,asset_name,asset_class,sector\n"
            "BTC,Bitcoin,CRYPTO,DIGITAL_ASSET\n"
            "ETH,Ethereum,CRYPTO,DIGITAL_ASSET\n"
            "SOL,Solana,CRYPTO,DIGITAL_ASSET\n"
            "ADA,Cardano,CRYPTO,DIGITAL_ASSET\n"
            "DOT,Polkadot,CRYPTO,DIGITAL_ASSET\n"
            "GDP,US GDP,MACRO,ECONOMIC\n"
            "CPIAUCSL,US CPI,MACRO,ECONOMIC\n"
            "UNRATE,US Unemployment,MACRO,ECONOMIC\n"
            "DGS10,US 10Y Treasury,MACRO,ECONOMIC\n",
            encoding="utf-8",
        )
        log.info("reference_assets_generated", path=str(ref_path))

    log.info("seed_complete")


if __name__ == "__main__":
    main()
