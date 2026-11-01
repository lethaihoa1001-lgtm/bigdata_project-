"""Bước 4 (Bạn 4): tổng hợp từ Parquet đã biến đổi và lưu 5 bảng tổng hợp -> parquet_output/

Dùng chung aggregations.build_tables() với notebook, không viết lại phép tổng hợp.
"""
import pandas as pd

from common import PARQUET_OUTPUT, TRANSFORMED_DIR, get_spark, write_parquet
from aggregations import build_tables

CORE_TABLES = ["city_summary", "segment_summary", "delivery_summary", "distance_summary", "hour_summary"]

spark = get_spark("Part4_Save_Parquet")

# 1. Đọc dữ liệu chi tiết từ kho Parquet (chỉ đọc cột cần thiết nhờ lưu theo cột)
df = spark.read.parquet(str(TRANSFORMED_DIR)).cache()
print("Số dòng:", df.count())

# 2. Tổng hợp (một nguồn duy nhất)
tables = build_tables(df)

# 3. Ghi Parquet bằng Spark (bảng nhỏ nên gộp thành 1 file mỗi bảng)
PARQUET_OUTPUT.mkdir(exist_ok=True)
for name in CORE_TABLES:
    write_parquet(tables[name], PARQUET_OUTPUT / name, num_files=1)

# 4. Kiểm tra: đọc lại bằng pandas
print("\n===== ĐỌC LẠI PARQUET (pandas) =====")
for name in CORE_TABLES:
    pdf = pd.read_parquet(PARQUET_OUTPUT / name)
    print(f"- {name}: {pdf.shape[0]} dòng, {pdf.shape[1]} cột")

# 5. Spark SQL trên bảng đọc từ kho Parquet
for name in CORE_TABLES:
    spark.read.parquet(str(PARQUET_OUTPUT / name)).createOrReplaceTempView(name)
print("\n===== SPARK SQL: TOP 5 THÀNH PHỐ THEO DOANH THU =====")
spark.sql("""
    SELECT City, Total_Orders, Total_Revenue, Revenue_Share_Percent
    FROM city_summary
    ORDER BY Total_Revenue DESC
    LIMIT 5
""").show(truncate=False)

print("Hoàn tất. Parquet nằm tại:", PARQUET_OUTPUT)
spark.stop()
