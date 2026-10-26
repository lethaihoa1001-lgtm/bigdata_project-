"""Bước 2 của phần 4: biến đổi, join -> fact_orders, fact_order_items và các bảng chiều (Parquet).

Ví dụ: spark-submit 03_transform.py --scale 1
Đầu ra: data/processed/<tag>/ (xem data/SCHEMA.md)
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import get_spark, interim_dir, processed_dir, timed


def write_partitioned(df, path):
    # repartition theo cột partition để mỗi partition ít file nhỏ.
    # Timestamp giữ mặc định của Spark (INT96) để Hive và Impala đọc được.
    (df.repartition("order_year", "order_month")
       .write.mode("overwrite").partitionBy("order_year", "order_month").parquet(str(path)))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", type=int, default=1)
    a = p.parse_args()

    spark = get_spark(f"transform_{a.scale}x")
    i, o = interim_dir(a.scale), processed_dir(a.scale)
    rd = lambda n: spark.read.parquet(str(i / n))
    orders, items, pays, reviews = rd("orders"), rd("order_items"), rd("order_payments"), rd("order_reviews")
    customers, products, sellers, trans = rd("customers"), rd("products"), rd("sellers"), rd("category_translation")

    # --- gom theo đơn ---
    items_agg = items.groupBy("order_id").agg(
        F.sum("price").alias("items_amount"),
        F.sum("freight_value").alias("freight_amount"),
        F.count("*").alias("item_count"),
    )
    pay_total = pays.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_total"),
        F.max("payment_installments").alias("max_installments"),
    )
    w_pay = Window.partitionBy("order_id").orderBy(F.col("payment_value").desc(), F.col("payment_sequential"))
    pay_main = (pays.withColumn("rn", F.row_number().over(w_pay)).filter("rn = 1")
                .select("order_id", F.col("payment_type").alias("payment_type_main")))
    w_rev = Window.partitionBy("order_id").orderBy(F.col("review_creation_date").desc(), F.col("review_id"))
    review_latest = (reviews.withColumn("rn", F.row_number().over(w_rev)).filter("rn = 1")
                     .select("order_id", "review_score"))

    # --- fact_orders ---
    deliv, est, purch = (F.col("order_delivered_customer_date"), F.col("order_estimated_delivery_date"),
                         F.col("order_purchase_timestamp"))
    day = lambda a_, b_: F.round((F.unix_timestamp(a_) - F.unix_timestamp(b_)) / 86400, 2)
    fact_orders = (
        orders
        .join(customers.select("customer_id", "customer_unique_id", "customer_city", "customer_state"),
              "customer_id", "left")
        .join(items_agg, "order_id", "left")
        .join(pay_total, "order_id", "left")
        .join(pay_main, "order_id", "left")
        .join(review_latest, "order_id", "left")
        .withColumn("total_amount", F.round(F.coalesce("items_amount", F.lit(0.0)) + F.coalesce("freight_amount", F.lit(0.0)), 2))
        .withColumn("delivery_days", day(deliv, purch))
        .withColumn("delay_days", day(deliv, est))
        .withColumn("is_late", F.when(deliv.isNotNull(), (deliv > est).cast("int")))
        .withColumn("order_year", F.year(purch))
        .withColumn("order_month", F.month(purch))
        .select(
            "order_id", "customer_id", "customer_unique_id", "customer_city", "customer_state", "order_status",
            "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "items_amount", "freight_amount", "item_count", "total_amount",
            "payment_total", "max_installments", "payment_type_main",
            "delivery_days", "delay_days", "is_late", "review_score",
            "order_year", "order_month",
        )
    )
    with timed("transform_fact_orders", a.scale):
        write_partitioned(fact_orders, o / "fact_orders")

    # --- dim_products (kèm tên danh mục tiếng Anh) ---
    dim_products = (
        products.join(trans, "product_category_name", "left")
        .withColumn("category_en", F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown")))
        .select("product_id", "product_category_name", "category_en", "product_name_length",
                "product_description_length", "product_photos_qty", "product_weight_g",
                "product_length_cm", "product_height_cm", "product_width_cm")
    )

    # --- fact_order_items (chỉ giữ món thuộc đơn có trong fact_orders) ---
    order_keys = spark.read.parquet(str(o / "fact_orders")).select("order_id", "order_status", "order_year", "order_month")
    fact_items = (
        items.join(dim_products.select("product_id", "category_en"), "product_id", "left")
        .join(sellers.select("seller_id", "seller_state"), "seller_id", "left")
        .join(order_keys, "order_id", "inner")
        .withColumn("category_en", F.coalesce("category_en", F.lit("unknown")))
        .select("order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price",
                "freight_value", "category_en", "seller_state", "order_status", "order_year", "order_month")
    )
    with timed("transform_fact_order_items", a.scale):
        write_partitioned(fact_items, o / "fact_order_items")

    # --- bảng chiều ---
    with timed("transform_dims", a.scale):
        customers.write.mode("overwrite").parquet(str(o / "dim_customers"))
        sellers.write.mode("overwrite").parquet(str(o / "dim_sellers"))
        dim_products.write.mode("overwrite").parquet(str(o / "dim_products"))

    spark.stop()


if __name__ == "__main__":
    main()
