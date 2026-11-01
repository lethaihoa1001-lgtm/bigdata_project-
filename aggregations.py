"""NƠI DUY NHẤT định nghĩa các phép tổng hợp.
aggregation.ipynb (hiển thị, xuất Excel) và save_parquet.py (lưu Parquet) cùng gọi build_tables(),
nên số liệu ở hai nơi luôn giống nhau.
"""
from pyspark.sql import functions as F


def build_tables(df):
    """df: DataFrame đã biến đổi (đọc từ data_parquet/transformed). Trả về dict {tên bảng: DataFrame}."""
    total_revenue = df.agg(F.sum("revenue")).collect()[0][0]

    overview = df.agg(
        F.count("*").alias("Total_Orders"),
        F.round(F.sum("revenue"), 2).alias("Total_Revenue"),
        F.round(F.avg("revenue"), 2).alias("Average_Revenue"),
        F.round(F.avg("Delivery_Duration_Hours"), 3).alias("Average_Delivery_Hours"),
        F.round(F.avg("Delivery_Distance_km"), 2).alias("Average_Distance_km"),
        F.sum("Quantity").alias("Total_Quantity"),
    )

    city_summary = (
        df.groupBy("City")
        .agg(
            F.count("*").alias("Total_Orders"),
            F.round(F.sum("revenue"), 2).alias("Total_Revenue"),
            F.round(F.avg("revenue"), 2).alias("Average_Revenue"),
            F.round(F.avg("Delivery_Duration_Hours"), 3).alias("Average_Delivery_Hours"),
        )
        .withColumn("Revenue_Share_Percent", F.round(F.col("Total_Revenue") / total_revenue * 100, 2))
        .orderBy(F.desc("Total_Revenue"))
    )

    segment_summary = (
        df.groupBy("order_segment")
        .agg(
            F.count("*").alias("Total_Orders"),
            F.round(F.sum("revenue"), 2).alias("Total_Revenue"),
            F.round(F.avg("revenue"), 2).alias("Average_Revenue"),
            F.round(F.avg("Quantity"), 2).alias("Average_Quantity"),
        )
        .orderBy(F.desc("Total_Revenue"))
    )

    delivery_summary = (
        df.groupBy("Delivery_Category")
        .agg(
            F.count("*").alias("Total_Orders"),
            F.round(F.avg("Delivery_Duration_Hours"), 3).alias("Average_Delivery_Hours"),
            F.round(F.avg("Delivery_Distance_km"), 2).alias("Average_Distance_km"),
            F.round(F.sum("revenue"), 2).alias("Total_Revenue"),
        )
        .orderBy(F.desc("Total_Orders"))
    )

    distance_summary = (
        df.groupBy("Distance_Category")
        .agg(
            F.count("*").alias("Total_Orders"),
            F.round(F.avg("Delivery_Distance_km"), 2).alias("Average_Distance_km"),
            F.round(F.avg("Delivery_Duration_Hours"), 3).alias("Average_Delivery_Hours"),
            F.round(F.sum("revenue"), 2).alias("Total_Revenue"),
        )
        .orderBy("Average_Distance_km")
    )

    hour_summary = (
        df.groupBy("Order_Hour")
        .agg(
            F.count("*").alias("Total_Orders"),
            F.round(F.sum("revenue"), 2).alias("Total_Revenue"),
            F.round(F.avg("revenue"), 2).alias("Average_Revenue"),
            F.round(F.avg("Delivery_Duration_Hours"), 3).alias("Average_Delivery_Hours"),
        )
        .orderBy("Order_Hour")
    )

    return {
        "overview": overview,
        "city_summary": city_summary,
        "top_city_revenue": city_summary.orderBy(F.desc("Total_Revenue")).limit(10),
        "top_city_orders": city_summary.orderBy(F.desc("Total_Orders")).limit(10),
        "city_revenue_analysis": city_summary.orderBy(F.desc("Revenue_Share_Percent")),
        "segment_summary": segment_summary,
        "delivery_summary": delivery_summary,
        "distance_summary": distance_summary,
        "hour_summary": hour_summary,
        "top_hours": hour_summary.orderBy(F.desc("Total_Orders")).limit(10),
    }
