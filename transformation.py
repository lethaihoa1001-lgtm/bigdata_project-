"""Bước 2 (Bạn 2): đọc Parquet đã làm sạch, biến đổi, GHI PARQUET PHÂN VÙNG THEO THÁNG -> data_parquet/transformed/

Logic biến đổi giữ nguyên như bản trước; điểm khác: đọc/ghi Parquet bằng Spark, có phân vùng (partition).
"""
from common import CLEAN_DIR, TRANSFORMED_DIR, get_spark, write_parquet  # import trước pyspark.sql.functions

from pyspark.sql.functions import col, day, hour, month, to_date, to_timestamp, when

spark = get_spark("Data_Transformation")

# 1. Đọc dữ liệu đã làm sạch (output của bước 1)
df_clean = spark.read.parquet(str(CLEAN_DIR))

# 2. Trích xuất thời gian
df_transform = (
    df_clean
    .withColumn("Order_Timestamp", to_timestamp(col("Order_Time"), "dd-MM-yyyy HH:mm"))
    .withColumn("Order_Date", to_date(col("Order_Timestamp")))
    .withColumn("Order_Hour", hour(col("Order_Timestamp")))
    .withColumn("Order_Day", day(col("Order_Timestamp")))
    .withColumn("Order_Month", month(col("Order_Timestamp")))
)

# 3. Phút -> giờ
df_transform = df_transform.withColumn("Delivery_Duration_Hours", col("Delivery_Duration_Minutes") / 60)

# 4. Phân loại khoảng cách
df_transform = df_transform.withColumn(
    "Distance_Category",
    when(col("Delivery_Distance_km") < 3, "Near")
    .when(col("Delivery_Distance_km") <= 5, "Medium")
    .otherwise("Far"),
)

# 5. Phân loại thời gian giao
df_transform = df_transform.withColumn(
    "Delivery_Category",
    when(col("Delivery_Duration_Minutes") <= 30, "Fast")
    .when(col("Delivery_Duration_Minutes") <= 45, "Normal")
    .otherwise("Slow"),
)

# 6. Kiểm tra
print("===== SCHEMA SAU TRANSFORMATION =====")
df_transform.printSchema()
df_transform.select(
    "Order_ID", "Order_Time", "Order_Date", "Order_Hour", "Order_Month",
    "Delivery_Duration_Minutes", "Delivery_Duration_Hours", "Delivery_Distance_km",
    "Distance_Category", "Delivery_Category",
).show(10, truncate=False)

# 7. Ghi Parquet phân vùng theo Order_Month: mỗi tháng một thư mục Order_Month=<ngày>/
writer = write_parquet(df_transform, TRANSFORMED_DIR, partition_by=["Order_Day"])
print(f"Đã lưu Parquet (phân vùng theo Order_Day, ghi bởi {writer}) tại: {TRANSFORMED_DIR}")

spark.stop()
