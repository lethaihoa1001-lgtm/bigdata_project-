"""Vẽ bộ biểu đồ khám phá dữ liệu (EDA) cho dataset Food Order Delivery.

Chạy:  python draw_eda_charts.py
- Ưu tiên đọc Parquet đã biến đổi (data_parquet/transformed); không có thì đọc Order_delivery.csv.
- Không phụ thuộc cột do pipeline tạo: mọi cột phụ (giờ, ngày, phân khúc...) được tính lại từ cột gốc,
  nên chạy được trên mọi máy chỉ cần pandas, pyarrow, matplotlib.
- Kết quả: thư mục charts/*.png và charts/charts_overview.pdf
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter

BASE = Path(__file__).resolve().parent
OUT = BASE / "charts"
OUT.mkdir(exist_ok=True)

# ---------- 1. Nạp dữ liệu ----------
parquet_dir = BASE / "data_parquet" / "transformed"
csv_file = BASE / "Order_delivery.csv"
if parquet_dir.exists():
    df = pd.read_parquet(parquet_dir)
    print("Đọc từ Parquet:", parquet_dir)
else:
    df = pd.read_csv(csv_file)
    print("Đọc từ CSV:", csv_file)

# Tính lại các cột phụ từ cột gốc (không phụ thuộc pipeline)
ot = pd.to_datetime(df["Order_Time"], format="%d-%m-%Y %H:%M")
df["hour"], df["day"] = ot.dt.hour, ot.dt.day
df["revenue"] = df["Total_Price"]
df["segment"] = pd.cut(df["revenue"], [-np.inf, 199.999999, 500, np.inf], labels=["Low", "Medium", "High"])
df["dist_cat"] = pd.cut(df["Delivery_Distance_km"], [-np.inf, 2.999999, 5, np.inf], labels=["Near", "Medium", "Far"])
df["deliv_cat"] = pd.cut(df["Delivery_Duration_Minutes"], [-np.inf, 30, 45, np.inf], labels=["Fast", "Normal", "Slow"])
df["unit_price"] = df["Total_Price"] / df["Quantity"]

# ---------- 2. Style chung ----------
PAL = {"main": "#2E6F95", "accent": "#E07A5F", "ok": "#3D9970", "warn": "#E9C46A", "bad": "#C0392B", "grey": "#9AA5B1"}
STATUS_COL = {"Delivered": PAL["ok"], "In Transit": PAL["warn"], "Cancelled": PAL["bad"]}
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": 0.25, "figure.dpi": 110, "axes.titleweight": "bold"})
fmt_m = FuncFormatter(lambda v, _: f"{v / 1e6:.1f} tr")   # triệu
fmt_k = FuncFormatter(lambda v, _: f"{v / 1e3:.0f}k")     # nghìn
figs = []


def done(fig, name, note=None):
    if note:
        fig.text(0.01, 0.005, note, fontsize=8, color="#555", ha="left", va="bottom")
    fig.tight_layout(rect=(0, 0.03 if note else 0, 1, 1))
    fig.savefig(OUT / f"{name}.png", dpi=150)
    figs.append(fig)
    print("  đã vẽ", name)


# ---------- 3. Các biểu đồ ----------
# 01. Đơn theo ngày: làm nổi bật 2 ngày không đủ 24 giờ
fig, ax = plt.subplots(figsize=(9, 4.2))
d = df.groupby("day").size()
colors = [PAL["grey"] if k in (1, 16) else PAL["main"] for k in d.index]
ax.bar(d.index, d.values, color=colors)
ax.set_xticks(d.index); ax.set_xlabel("Ngày trong tháng 6/2025"); ax.set_ylabel("Số đơn")
ax.set_title("Số đơn theo ngày (ngày 1 và 16 là ngày chưa đủ 24 giờ)")
ax.yaxis.set_major_formatter(fmt_k)
done(fig, "01_orders_per_day", "Dữ liệu chạy từ 01/06 10:00 đến 16/06 10:00 nên 2 ngày đầu/cuối thấp là do cắt dữ liệu, không phải xu hướng.")

# 02. Đơn theo giờ (trục y bắt đầu từ 0 để không phóng đại)
fig, ax = plt.subplots(figsize=(9, 4.2))
h = df.groupby("hour").size()
ax.plot(h.index, h.values, marker="o", color=PAL["main"])
ax.axhline(h.mean(), color=PAL["accent"], ls="--", label=f"Trung bình {h.mean():,.0f}")
ax.set_ylim(0, h.max() * 1.15); ax.set_xticks(range(24))
ax.set_xlabel("Giờ đặt hàng"); ax.set_ylabel("Số đơn"); ax.legend()
ax.set_title("Số đơn theo giờ trong ngày")
done(fig, "02_orders_by_hour", f"Giờ cao nhất {h.max():,} đơn, thấp nhất {h.min():,} đơn (chênh {(h.max() - h.min()) / h.mean():.1%} so với TB): gần như phân bố đều, không có giờ cao điểm.")

# 03. Doanh thu theo thành phố, tách theo trạng thái đơn
fig, ax = plt.subplots(figsize=(9, 4.6))
pv = df.pivot_table(index="City", columns="Order_Status", values="revenue", aggfunc="sum")[["Delivered", "In Transit", "Cancelled"]]
pv = pv.loc[pv.sum(axis=1).sort_values(ascending=False).index]
bottom = np.zeros(len(pv))
for s in pv.columns:
    ax.bar(pv.index, pv[s], bottom=bottom, label=s, color=STATUS_COL[s]); bottom += pv[s].values
ax.yaxis.set_major_formatter(fmt_m); ax.set_ylabel("Doanh thu")
ax.set_title("Doanh thu theo thành phố, tách theo trạng thái đơn"); ax.legend(title="Trạng thái")
done(fig, "03_city_revenue_by_status", "Doanh thu 'thực nhận' chỉ nên tính đơn Delivered; hiện pipeline đang cộng cả đơn Cancelled và In Transit.")

# 04. Cơ cấu trạng thái: số đơn vs doanh thu
fig, axes = plt.subplots(1, 2, figsize=(9, 4.2))
cnt = df["Order_Status"].value_counts()[["Delivered", "In Transit", "Cancelled"]]
rev = df.groupby("Order_Status")["revenue"].sum()[["Delivered", "In Transit", "Cancelled"]]
for ax, data, ttl in zip(axes, (cnt, rev), ("Theo số đơn", "Theo doanh thu")):
    ax.pie(data, labels=data.index, autopct="%1.1f%%", startangle=90, colors=[STATUS_COL[k] for k in data.index],
           wedgeprops={"width": 0.45, "edgecolor": "white"})
    ax.set_title(ttl)
fig.suptitle("Cơ cấu trạng thái đơn hàng", fontweight="bold")
done(fig, "04_status_share", f"Doanh thu bị 'thổi phồng' khoảng {(rev['Cancelled'] + rev['In Transit']) / rev.sum():.1%} nếu tính cả đơn hủy và đang giao.")

# 05. Tỷ trọng đơn theo thành phố so với mức chia đều
fig, ax = plt.subplots(figsize=(9, 4.2))
cs = (df["City"].value_counts(normalize=True) * 100).sort_values(ascending=False)
ax.bar(cs.index, cs.values, color=PAL["main"])
ax.axhline(100 / len(cs), color=PAL["accent"], ls="--", label=f"Chia đều ({100 / len(cs):.2f}%)")
ax.set_ylim(0, cs.max() * 1.25); ax.set_ylabel("% tổng số đơn"); ax.legend()
for i, v in enumerate(cs.values):
    ax.text(i, v + 0.2, f"{v:.2f}%", ha="center", fontsize=9)
ax.set_title("Tỷ trọng số đơn theo thành phố")
done(fig, "05_city_share", f"Chênh lệch giữa thành phố cao nhất và thấp nhất chỉ {cs.max() - cs.min():.2f} điểm phần trăm.")

# 06. Phân khúc: nhiều đơn nhưng ít doanh thu (biểu đồ có thông tin nhất)
fig, ax = plt.subplots(figsize=(8.5, 4.4))
seg = df.groupby("segment", observed=True).agg(orders=("Order_ID", "count"), rev=("revenue", "sum"))
seg = seg.loc[["Low", "Medium", "High"]]
x = np.arange(3); w = 0.38
b1 = ax.bar(x - w / 2, seg["orders"] / seg["orders"].sum() * 100, w, label="% số đơn", color=PAL["main"])
b2 = ax.bar(x + w / 2, seg["rev"] / seg["rev"].sum() * 100, w, label="% doanh thu", color=PAL["accent"])
for b in list(b1) + list(b2):
    ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.8, f"{b.get_height():.1f}%", ha="center", fontsize=9)
ax.set_xticks(x); ax.set_xticklabels(["Low (<200)", "Medium (200-500)", "High (>500)"])
ax.set_ylabel("%"); ax.set_ylim(0, 60); ax.legend(); ax.set_title("Phân khúc đơn: tỷ trọng số đơn so với tỷ trọng doanh thu")
done(fig, "06_segment_orders_vs_revenue", "Nhóm High chỉ ~12% số đơn nhưng đóng góp ~27% doanh thu; nhóm Low ngược lại. Đây là điểm đáng khai thác nhất.")

# 07. Phân bố thời gian giao
fig, ax = plt.subplots(figsize=(9, 4.2))
ax.hist(df["Delivery_Duration_Minutes"], bins=range(15, 62, 3), color=PAL["main"], edgecolor="white")
for v, lab in ((30, "30 phút (Fast)"), (45, "45 phút (Normal)")):
    ax.axvline(v, color=PAL["accent"], ls="--"); ax.text(v + 0.3, ax.get_ylim()[1] * 0.93, lab, color=PAL["accent"], fontsize=9)
ax.set_xlabel("Thời gian giao (phút)"); ax.set_ylabel("Số đơn"); ax.yaxis.set_major_formatter(fmt_k)
ax.set_title("Phân bố thời gian giao hàng")
done(fig, "07_duration_hist", "Thời gian giao phân bố gần đều từ 15 đến 60 phút (dấu hiệu dữ liệu sinh ngẫu nhiên), không có đỉnh như dữ liệu thực.")

# 08. Phân bố khoảng cách
fig, ax = plt.subplots(figsize=(9, 4.2))
ax.hist(df["Delivery_Distance_km"], bins=40, color=PAL["main"], edgecolor="white")
for v, lab in ((3, "3 km"), (5, "5 km")):
    ax.axvline(v, color=PAL["accent"], ls="--"); ax.text(v + 0.03, ax.get_ylim()[1] * 0.93, lab, color=PAL["accent"], fontsize=9)
ax.set_xlabel("Khoảng cách giao (km)"); ax.set_ylabel("Số đơn"); ax.yaxis.set_major_formatter(fmt_k)
n_far = int((df["dist_cat"] == "Far").sum())
ax.set_title("Phân bố khoảng cách giao hàng")
done(fig, "08_distance_hist", f"Chỉ {n_far} đơn thuộc nhóm Far (>5 km): các số trung bình của nhóm này kém tin cậy.")

# 09. Thời gian giao theo khoảng cách (kiểm tra giả định 'xa thì lâu')
fig, ax = plt.subplots(figsize=(8.5, 4.4))
bins = pd.cut(df["Delivery_Distance_km"], [0, 1, 2, 3, 4, 5, 6])
g = df.groupby(bins, observed=True)["Delivery_Duration_Minutes"].agg(["mean", "std", "count"])
labels = [f"{int(i.left)}-{int(i.right)} km" for i in g.index]
ax.bar(labels, g["mean"], yerr=g["std"], capsize=4, color=PAL["main"], alpha=0.9)
ax.set_ylim(0, 65); ax.set_ylabel("Thời gian giao TB (phút)")
for i, (m, c) in enumerate(zip(g["mean"], g["count"])):
    ax.text(i, 2, f"{m:.1f}\nn={c:,}", ha="center", color="white", fontsize=8)
r = df["Delivery_Distance_km"].corr(df["Delivery_Duration_Minutes"])
ax.set_title(f"Thời gian giao theo khoảng cách (hệ số tương quan r = {r:.3f})")
done(fig, "09_duration_by_distance", "Đường 'phẳng': khoảng cách gần như không ảnh hưởng thời gian giao, trái với thực tế. Thanh sai số là độ lệch chuẩn.")

# 10. Thời gian giao theo giao thông và phương tiện
fig, ax = plt.subplots(figsize=(8.5, 4.4))
pv2 = df.pivot_table(index="Traffic_Level", columns="Driver_Vehicle", values="Delivery_Duration_Minutes", aggfunc="mean").loc[["Low", "Medium", "High"]]
pv2.plot.bar(ax=ax, rot=0, color=[PAL["main"], PAL["accent"], PAL["ok"]])
ax.set_ylim(0, 60); ax.set_xlabel("Mức giao thông"); ax.set_ylabel("Thời gian giao TB (phút)")
ax.set_title("Thời gian giao theo mức giao thông và phương tiện"); ax.legend(title="Phương tiện", ncol=3, loc="lower center")
done(fig, "10_duration_traffic_vehicle", "Mọi cột gần bằng nhau (~37,5 phút): giao thông và phương tiện không ảnh hưởng. Biểu đồ này dùng để chứng minh dữ liệu mô phỏng.")

# 11. Giá theo số lượng (quan hệ duy nhất có thật) + phân bố đơn giá
fig, axes = plt.subplots(1, 2, figsize=(10, 4.4))
data = [df.loc[df["Quantity"] == q, "Total_Price"] for q in range(1, 6)]
axes[0].boxplot(data, tick_labels=[1, 2, 3, 4, 5], patch_artist=True, boxprops={"facecolor": "#B7D3E6"}, medianprops={"color": PAL["accent"]})
axes[0].set_xlabel("Số lượng món"); axes[0].set_ylabel("Tổng giá"); axes[0].set_title("Tổng giá theo số lượng")
axes[1].hist(df["unit_price"], bins=30, color=PAL["main"], edgecolor="white")
axes[1].set_xlabel("Đơn giá = Total_Price / Quantity"); axes[1].set_ylabel("Số đơn"); axes[1].yaxis.set_major_formatter(fmt_k)
axes[1].set_title("Phân bố đơn giá (đều từ 30 đến 150)")
done(fig, "11_price_vs_quantity", "Total_Price = Quantity x đơn giá ngẫu nhiên 30-150, đơn giá không phụ thuộc món ăn: đây là mối quan hệ duy nhất có thật trong dữ liệu (r = 0,74).")

# 12. Doanh thu theo món
fig, ax = plt.subplots(figsize=(9, 4.4))
it = df.groupby("Item_Name")["revenue"].sum().sort_values()
ax.barh(it.index, it.values, color=PAL["main"])
ax.xaxis.set_major_formatter(fmt_m); ax.set_xlabel("Doanh thu"); ax.set_title("Doanh thu theo món")
for i, v in enumerate(it.values):
    ax.text(v, i, f" {v / 1e6:.2f} tr", va="center", fontsize=9)
ax.set_xlim(0, it.max() * 1.15)
done(fig, "12_revenue_by_item", f"9 món gần như ngang nhau (chênh {(it.max() - it.min()) / it.mean():.1%} so với TB).")

# 13. Bản đồ phân bố khách hàng
fig, ax = plt.subplots(figsize=(7, 7))
cmap = plt.get_cmap("tab10")
for i, (city, g2) in enumerate(df.groupby("City")):
    s = g2.sample(min(len(g2), 2500), random_state=1)
    ax.scatter(s["Customer_Lon"], s["Customer_Lat"], s=5, alpha=0.5, color=cmap(i), label=city)
    off = (-34, -16) if city == "Giza" else (8, 8)   # tránh chồng nhãn Cairo/Giza (hai thành phố rất gần nhau)
    ax.annotate(city, (g2["Customer_Lon"].mean(), g2["Customer_Lat"].mean()), xytext=off, textcoords="offset points", fontsize=9, weight="bold")
ax.set_xlabel("Kinh độ"); ax.set_ylabel("Vĩ độ"); ax.set_title("Vị trí khách hàng (mẫu 2.500 đơn mỗi thành phố)")
ax.legend(markerscale=3, fontsize=8, loc="lower left")
done(fig, "13_customer_map", "7 cụm tách biệt, mỗi cụm chỉ rộng vài km quanh tâm thành phố; muốn xem rõ thì dùng bản đồ tương tác (folium/plotly).")

# 14. Ma trận tương quan
fig, ax = plt.subplots(figsize=(6.5, 5.5))
cols = ["Quantity", "Total_Price", "Delivery_Duration_Minutes", "Delivery_Distance_km", "hour", "day"]
cm = df[cols].corr()
im = ax.imshow(cm, cmap="RdBu_r", vmin=-1, vmax=1)
ax.set_xticks(range(len(cols))); ax.set_yticks(range(len(cols)))
short = ["Số lượng", "Tổng giá", "Thời gian giao", "Khoảng cách", "Giờ", "Ngày"]
ax.set_xticklabels(short, rotation=35, ha="right"); ax.set_yticklabels(short); ax.grid(False)
for i in range(len(cols)):
    for j in range(len(cols)):
        ax.text(j, i, f"{cm.iloc[i, j]:.2f}", ha="center", va="center", fontsize=8, color="white" if abs(cm.iloc[i, j]) > 0.5 else "black")
fig.colorbar(im, ax=ax, shrink=0.8); ax.set_title("Ma trận tương quan")
done(fig, "14_correlation", "Chỉ cặp Số lượng - Tổng giá có tương quan (0,74); mọi cặp khác xấp xỉ 0.")

with PdfPages(OUT / "charts_overview.pdf") as pdf:
    for f in figs:
        pdf.savefig(f)
print(f"Xong: {len(figs)} biểu đồ trong {OUT}")
