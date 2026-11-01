"""Đối chiếu parquet_output với số liệu trong README (mục 7) để chắc chắn việc nâng cấp không làm đổi kết quả.
Chỉ cần pandas và pyarrow, không cần Spark. Chạy: python check_results.py
"""
import pandas as pd

from common import PARQUET_OUTPUT


def load(name):
    return pd.read_parquet(PARQUET_OUTPUT / name)


EXPECTED_COUNTS = {
    "segment_summary": ("order_segment", {"Medium": 44838, "High": 12320, "Low": 42844}),
    "delivery_summary": ("Delivery_Category", {"Normal": 48411, "Fast": 27414, "Slow": 24177}),
    "distance_summary": ("Distance_Category", {"Near": 77069, "Medium": 22779, "Far": 154}),
}
EXPECTED_TOTAL_ORDERS = 100002
EXPECTED_TOTAL_REVENUE = 26892805.61  # Total_Revenue trong notebook gốc: 2.689280561E7


def main():
    bad = 0

    def report(label, ok, detail=""):
        nonlocal bad
        print(f"[{'OK ' if ok else 'LỆCH'}] {label} {detail}")
        bad += 0 if ok else 1

    city = load("city_summary")
    report("Tổng số đơn", int(city["Total_Orders"].sum()) == EXPECTED_TOTAL_ORDERS,
           f"(có {int(city['Total_Orders'].sum())}, mong đợi {EXPECTED_TOTAL_ORDERS})")
    revenue = float(city["Total_Revenue"].sum())
    report("Tổng doanh thu", abs(revenue - EXPECTED_TOTAL_REVENUE) < 5,
           f"(có {revenue:,.2f}, mong đợi khoảng {EXPECTED_TOTAL_REVENUE:,.2f})")
    for table, (key, expected) in EXPECTED_COUNTS.items():
        got = dict(zip(load(table)[key], load(table)["Total_Orders"]))
        for k, v in expected.items():
            report(f"{table} / {k}", int(got.get(k, -1)) == v, f"(có {got.get(k)}, mong đợi {v})")
    print("\nTất cả khớp." if bad == 0 else f"\nCó {bad} chỗ lệch: kiểm tra lại bước làm sạch hoặc biến đổi.")


if __name__ == "__main__":
    main()
