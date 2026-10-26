"""Bước 3 của phần 4: tổng hợp -> bảng agg_* (Parquet) và in kết quả để chụp ảnh báo cáo.

Ví dụ: spark-submit 04_aggregate.py --scale 1
"""
import argparse

from pyspark.sql import functions as F

from common import get_spark, processed_dir, timed

EXCLUDED_STATUS = ["canceled", "unavailable"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", type=int, default=1)
    a = p.parse_args()

    spark = get_spark(f"aggregate_{a.scale}x")
    o = processed_dir(a.scale)
    orders = spark.read.parquet(str(o / "fact_orders"))
    items = spark.read.parquet(str(o / "fact_order_items"))
    valid = orders.filter(~F.col("order_status").isin(EXCLUDED_STATUS))
    valid_items = items.filter(~F.col("order_status").isin(EXCLUDED_STATUS))

    by_month = (valid.groupBy("order_year", "order_month")
                .agg(F.count("*").alias("orders"), F.round(F.sum("total_amount"), 2).alias("revenue"),
                     F.round(F.avg("total_amount"), 2).alias("avg_order_value"))
                .orderBy("order_year", "order_month"))

    by_state = (valid.groupBy("customer_state")
                .agg(F.count("*").alias("orders"), F.round(F.sum("total_amount"), 2).alias("revenue"),
                     F.round(F.avg("is_late"), 4).alias("late_rate"),
                     F.round(F.avg("review_score"), 2).alias("avg_review"))
                .orderBy(F.col("revenue").desc()))

    by_category = (valid_items.groupBy("category_en")
                   .agg(F.count("*").alias("items_sold"), F.round(F.sum("price"), 2).alias("revenue"))
                   .orderBy(F.col("revenue").desc()))

    by_payment = (valid.groupBy("payment_type_main")
                  .agg(F.count("*").alias("orders"), F.round(F.sum("total_amount"), 2).alias("revenue"),
                       F.round(F.avg("max_installments"), 2).alias("avg_installments"))
                  .orderBy(F.col("orders").desc()))

    results = {
        "agg_revenue_by_month": by_month,
        "agg_revenue_by_state": by_state,
        "agg_revenue_by_category": by_category,
        "agg_payment_type": by_payment,
    }
    for name, df in results.items():
        with timed(f"aggregate_{name}", a.scale):
            df.coalesce(1).write.mode("overwrite").parquet(str(o / name))
        print(f"\n=== {name} ===")
        spark.read.parquet(str(o / name)).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
