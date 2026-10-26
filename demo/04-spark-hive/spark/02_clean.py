"""Bước 1 của phần 4: làm sạch dữ liệu thô -> Parquet ở data/interim/<tag>/.

Ví dụ: spark-submit 02_clean.py --scale 1
Ghi số dòng trước/sau vào data/interim/<tag>/_cleaning_metrics.json và thời gian vào results/spark_timings.csv.
"""
import argparse
import json

from pyspark.sql import functions as F

from common import TABLES, get_spark, interim_dir, raw_dir, read_csv, timed


def ts(c):
    return F.to_timestamp(F.col(c)).alias(c)


def clean_orders(df):
    df = df.select(
        "order_id", "customer_id",
        F.lower(F.trim(F.col("order_status"))).alias("order_status"),
        ts("order_purchase_timestamp"), ts("order_approved_at"), ts("order_delivered_carrier_date"),
        ts("order_delivered_customer_date"), ts("order_estimated_delivery_date"),
    )
    df = df.dropna(subset=["order_id", "customer_id", "order_status", "order_purchase_timestamp"])
    df = df.dropDuplicates(["order_id"])
    delivered_no_date = (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull()
    time_travel = F.coalesce(F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"), F.lit(False))
    return df.filter(~delivered_no_date).filter(~time_travel)


def clean_order_items(df):
    df = df.select(
        "order_id", F.col("order_item_id").cast("int").alias("order_item_id"), "product_id", "seller_id",
        ts("shipping_limit_date"),
        F.col("price").cast("double").alias("price"),
        F.col("freight_value").cast("double").alias("freight_value"),
    )
    df = df.dropna(subset=["order_id", "order_item_id", "product_id", "seller_id", "price"])
    df = df.fillna({"freight_value": 0.0})
    return df.filter((F.col("price") > 0) & (F.col("freight_value") >= 0)).dropDuplicates(["order_id", "order_item_id"])


def clean_order_payments(df):
    df = df.select(
        "order_id", F.col("payment_sequential").cast("int").alias("payment_sequential"),
        F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
        F.col("payment_installments").cast("int").alias("payment_installments"),
        F.col("payment_value").cast("double").alias("payment_value"),
    )
    df = df.dropna(subset=["order_id", "payment_sequential", "payment_value"])
    df = df.filter((F.col("payment_value") >= 0) & (F.col("payment_type") != "not_defined"))
    return df.dropDuplicates(["order_id", "payment_sequential"])


def clean_order_reviews(df):
    df = df.select(
        "review_id", "order_id", F.col("review_score").cast("int").alias("review_score"),
        ts("review_creation_date"), ts("review_answer_timestamp"),
    )
    df = df.dropna(subset=["review_id", "order_id", "review_score"])
    return df.filter(F.col("review_score").between(1, 5)).dropDuplicates(["review_id", "order_id"])


def norm_city(c):
    return F.regexp_replace(F.lower(F.trim(F.col(c))), r"\s+", " ").alias(c)


def clean_customers(df):
    df = df.select(
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        norm_city("customer_city"), F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
    )
    return df.dropna(subset=["customer_id", "customer_unique_id"]).dropDuplicates(["customer_id"])


def clean_sellers(df):
    df = df.select(
        "seller_id", "seller_zip_code_prefix",
        norm_city("seller_city"), F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
    )
    return df.dropna(subset=["seller_id"]).dropDuplicates(["seller_id"])


def clean_products(df):
    df = df.select(
        "product_id",
        F.lower(F.trim(F.col("product_category_name"))).alias("product_category_name"),
        F.col("product_name_lenght").cast("int").alias("product_name_length"),  # sửa lỗi chính tả của nguồn
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.col("product_weight_g").cast("double").alias("product_weight_g"),
        F.col("product_length_cm").cast("double").alias("product_length_cm"),
        F.col("product_height_cm").cast("double").alias("product_height_cm"),
        F.col("product_width_cm").cast("double").alias("product_width_cm"),
    )
    return df.dropna(subset=["product_id"]).dropDuplicates(["product_id"])


def clean_category_translation(df):
    df = df.select(
        F.lower(F.trim(F.col("product_category_name"))).alias("product_category_name"),
        F.trim(F.col("product_category_name_english")).alias("product_category_name_english"),
    )
    return df.dropna().dropDuplicates(["product_category_name"])


CLEANERS = {
    "orders": clean_orders,
    "order_items": clean_order_items,
    "order_payments": clean_order_payments,
    "order_reviews": clean_order_reviews,
    "customers": clean_customers,
    "sellers": clean_sellers,
    "products": clean_products,
    "category_translation": clean_category_translation,
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", type=int, default=1)
    a = p.parse_args()

    spark = get_spark(f"clean_{a.scale}x")
    src, out = raw_dir(a.scale), interim_dir(a.scale)
    metrics = {}

    for name, fname in TABLES.items():
        raw = read_csv(spark, src / fname)
        before = raw.count()
        cleaned = CLEANERS[name](raw)
        with timed(f"clean_{name}", a.scale):
            cleaned.write.mode("overwrite").parquet(str(out / name))
        after = spark.read.parquet(str(out / name)).count()
        metrics[name] = {"before": before, "after": after, "removed": before - after}
        print(f"[CLEAN] {name:22s} {before:>12,} -> {after:>12,}  (loại {before - after:,})")

    (out).mkdir(parents=True, exist_ok=True)
    with open(out / "_cleaning_metrics.json", "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)
    spark.stop()


if __name__ == "__main__":
    main()
