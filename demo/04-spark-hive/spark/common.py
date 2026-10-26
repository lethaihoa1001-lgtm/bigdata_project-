"""Tiện ích dùng chung cho các script PySpark của phần 4."""
import os
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

# demo/04-spark-hive/spark/common.py -> gốc repo là parents[3]
REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = Path(os.environ.get("DATA_DIR", REPO_ROOT / "data"))
RESULTS_DIR = Path(__file__).resolve().parents[1] / "results"

# tên logic -> tên file trong data/raw (geolocation không dùng)
TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def get_spark(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "16"))
        .getOrCreate()
    )


def tag(scale):
    return f"{scale}x"


def raw_dir(scale):
    return DATA_DIR / "raw" if scale == 1 else DATA_DIR / "raw_scaled" / tag(scale)


def interim_dir(scale):
    return DATA_DIR / "interim" / tag(scale)


def processed_dir(scale):
    return DATA_DIR / "processed" / tag(scale)


def read_csv(spark, path):
    """Đọc CSV Olist: toàn bộ cột là string (ép kiểu ở bước làm sạch).
    multiLine vì ô nhận xét trong order_reviews có xuống dòng."""
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(str(path))
    )
    # một số file có ký tự BOM ở đầu tên cột đầu tiên
    return df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])


@contextmanager
def timed(step, scale):
    """Đo thời gian một bước và ghi nối vào results/spark_timings.csv."""
    start = time.perf_counter()
    yield
    seconds = time.perf_counter() - start
    print(f"[TIME] {step} (scale={scale}x): {seconds:.1f}s")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    f = RESULTS_DIR / "spark_timings.csv"
    is_new = not f.exists()
    with f.open("a", encoding="utf-8") as fh:
        if is_new:
            fh.write("timestamp,step,scale,seconds\n")
        fh.write(f"{datetime.now().isoformat(timespec='seconds')},{step},{scale},{seconds:.2f}\n")
