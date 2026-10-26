"""Nhân bản dữ liệu Olist N lần để benchmark, kèm chèn một ít dữ liệu bẩn.

Mỗi bản sao thứ k (k >= 1): thêm hậu tố _k vào các ID, dịch mọi mốc thời gian k*shift ngày.
Bản k = 0 giữ nguyên dữ liệu gốc.

Ví dụ: spark-submit 01_scale_data.py --factor 10
Kết quả: data/raw_scaled/10x/<tên file gốc>/ (thư mục CSV, Spark đọc được như file).
"""
import argparse

from pyspark.sql import functions as F

from common import DATA_DIR, TABLES, get_spark, raw_dir, read_csv, tag

ID_COLS = {
    "orders": ["order_id", "customer_id"],
    "customers": ["customer_id", "customer_unique_id"],
    "order_items": ["order_id"],
    "order_payments": ["order_id"],
    "order_reviews": ["review_id", "order_id"],
}
TS_COLS = {
    "orders": [
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
}
TS_FMT = "yyyy-MM-dd HH:mm:ss"


def expand(spark, df, table, factor, shift_days):
    if table not in ID_COLS and table not in TS_COLS:
        return df  # products, sellers, translation giữ nguyên
    out = df.crossJoin(F.broadcast(spark.range(factor).withColumnRenamed("id", "k")))
    for c in ID_COLS.get(table, []):
        out = out.withColumn(
            c, F.when(F.col("k") == 0, F.col(c)).otherwise(F.concat(F.col(c), F.lit("_"), F.col("k").cast("string")))
        )
    for c in TS_COLS.get(table, []):
        out = out.withColumn(
            c, F.from_unixtime(F.unix_timestamp(F.col(c)) + F.col("k") * shift_days * 86400, TS_FMT)
        )
    return out.drop("k")


def inject_dirty(tables, rate):
    """Chèn lỗi có chủ đích để bước làm sạch có việc thật."""
    o = tables["orders"]
    dup = o.sample(rate, seed=1)  # dòng trùng order_id
    o = o.unionByName(dup).withColumn(
        "order_delivered_customer_date",  # đơn delivered nhưng mất ngày giao
        F.when((F.col("order_status") == "delivered") & (F.rand(7) < rate), F.lit(None).cast("string"))
        .otherwise(F.col("order_delivered_customer_date")),
    )
    tables["orders"] = o

    i = tables["order_items"]
    tables["order_items"] = i.withColumn(  # giá âm
        "price", F.when(F.rand(11) < rate, -F.col("price").cast("double")).otherwise(F.col("price").cast("double"))
    )

    c = tables["customers"]
    tables["customers"] = c.withColumn(  # tên thành phố sai định dạng
        "customer_city",
        F.when(F.rand(13) < rate, F.concat(F.lit("  "), F.upper(F.col("customer_city")), F.lit(" ")))
        .otherwise(F.col("customer_city")),
    )
    return tables


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--factor", type=int, required=True, help="số lần nhân bản, từ 2 trở lên")
    p.add_argument("--shift-days", type=int, default=30, help="số ngày dịch mốc thời gian giữa hai bản sao")
    p.add_argument("--dirty-rate", type=float, default=0.01, help="tỉ lệ dòng bẩn chèn thêm (0 để tắt)")
    a = p.parse_args()
    if a.factor < 2:
        raise SystemExit("factor >= 2. Với 1x hãy dùng thẳng data/raw.")

    spark = get_spark(f"scale_{a.factor}x")
    src = raw_dir(1)
    out_root = DATA_DIR / "raw_scaled" / tag(a.factor)

    tables = {}
    for name, fname in TABLES.items():
        df = read_csv(spark, src / fname)
        if name == "order_reviews":  # bỏ cột văn bản (nặng, có xuống dòng, không dùng)
            df = df.drop("review_comment_title", "review_comment_message")
        tables[name] = expand(spark, df, name, a.factor, a.shift_days)

    if a.dirty_rate > 0:
        tables = inject_dirty(tables, a.dirty_rate)

    for name, df in tables.items():
        target = out_root / TABLES[name]
        df.write.mode("overwrite").option("header", True).csv(str(target))
        print(f"[OK] {name} -> {target}")
    spark.stop()


if __name__ == "__main__":
    main()
