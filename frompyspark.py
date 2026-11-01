"""Bước 1 (Bạn 1): đọc CSV gốc, làm sạch, GHI PARQUET BẰNG SPARK -> data_parquet/clean/

Logic làm sạch giữ nguyên như bản trước; điểm khác: không dùng toPandas()/CSV nữa.
"""
import json

from common import CLEAN_DIR, RAW_CSV, REPORT_DIR, get_spark, write_parquet  # import trước pyspark.sql.functions

from pyspark.sql.functions import col, trim, when

spark = get_spark("BigData_Part1_Ingestion_Cleaning")

# 1. Đọc dữ liệu gốc
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(str(RAW_CSV))
raw_count = df_raw.count()
print(f"=== Số lượng bản ghi ban đầu: {raw_count} ===")

# 2. Làm sạch dữ liệu
df_clean = (
    df_raw
    .dropDuplicates()
    .na.drop(subset=["Order_ID", "User_ID"])
    .withColumn("Item_Name", trim(col("Item_Name")))
    .withColumn("City", trim(col("City")))
    .withColumn("revenue", col("Total_Price"))
)

df_clean = (
    df_clean
    .na.fill({"Quantity": 0, "revenue": 0.0})
    .filter((col("Quantity") >= 0) & (col("revenue") >= 0))
)

df_clean = df_clean.withColumn(
    "order_segment",
    when(col("revenue") < 200, "Low")
    .when((col("revenue") >= 200) & (col("revenue") <= 500), "Medium")
    .otherwise("High"),
)

# 3. Ghi Parquet bằng Spark (giữ nguyên kiểu dữ liệu, nén, đọc nhanh ở các bước sau)
writer = write_parquet(df_clean, CLEAN_DIR)

# Đếm lại từ file Parquet vừa ghi (đọc rất nhanh nhờ siêu dữ liệu), không tính lại toàn bộ phép biến đổi
clean_count = spark.read.parquet(str(CLEAN_DIR)).count()
print(f"=== Số lượng bản ghi sau khi làm sạch: {clean_count} (loại {raw_count - clean_count}) ===")

REPORT_DIR.mkdir(exist_ok=True)
with open(REPORT_DIR / "cleaning_report.json", "w", encoding="utf-8") as fh:
    json.dump({"raw_rows": raw_count, "clean_rows": clean_count, "removed": raw_count - clean_count,
               "parquet_writer": writer}, fh, indent=2)
print(f"Đã lưu Parquet tại: {CLEAN_DIR}")

spark.stop()
