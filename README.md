# Dự án Big Data: Phân tích đơn hàng giao đồ ăn bằng PySpark

> **Phần giữa kì** (dữ liệu giao đồ ăn `Order_delivery.csv`). Mọi file nằm trong thư mục `giua-ki/`. Phần cuối kì nằm ở thư mục gốc của repo.

Tài liệu này giới thiệu dự án: **làm gì, ai làm gì, mỗi bước tạo ra file nào, dữ liệu nằm ở đâu và kết quả chính**.
Cách cài môi trường để chạy code nằm ở file **`README_SETUP.md`**.

> **Đã nâng cấp phần lưu trữ Big Data:** các bước ghi **Parquet bằng Spark** (có phân vùng theo tháng) thay vì CSV qua pandas. Xem `NANG_CAP_BIG_DATA.md`.

---

## 1. Dự án này làm gì?

- **Dữ liệu gốc:** `Order_delivery.csv`, khoảng **100.002 đơn hàng** giao đồ ăn tại các thành phố của Ai Cập (Cairo, Alexandria, Tanta, Zagazig, Assiut, Mansoura...).
- **Nguồn dữ liệu:** *Food Order Delivery Dataset* (tác giả srisyra02, Kaggle): https://www.kaggle.com/datasets/srisyra02/food-order-delivery-dataset . Trang Kaggle gắn thẻ "synthetic" nên đây là dữ liệu mô phỏng. Tải file CSV về, đổi tên thành `Order_delivery.csv` nếu cần, đặt cùng thư mục với các file `.py`.
- **Mục tiêu:** làm sạch dữ liệu, biến đổi, tổng hợp thống kê, lưu kết quả, rồi **vẽ biểu đồ và viết báo cáo**.
- **Công cụ:** Python, PySpark (xử lý dữ liệu lớn), Pandas, Parquet, Matplotlib.

---

## 2. Ai làm gì? Tạo ra file nào?

| Bạn | Nội dung | File code | Đọc vào | **Tạo ra** | Trạng thái |
|---|---|---|---|---|---|
| **Bạn 1** | Đọc & làm sạch dữ liệu | `frompyspark.py` | `Order_delivery.csv` | `data_parquet/clean/` | Xong |
| **Bạn 2** | Biến đổi dữ liệu | `transformation.py` | `data_parquet/clean/` | `data_parquet/transformed/` | Xong |
| **Bạn 3** | Tổng hợp & thống kê | `aggregation.ipynb` | `data_parquet/transformed/` | thư mục `output/` (10 file Excel) | Xong |
| **Bạn 4** | Lưu vào Parquet | `save_parquet.py` | `data_parquet/transformed/` | thư mục `parquet_output/` (5 bảng Parquet) | Xong |
| **Bạn 5** | Trực quan hóa & báo cáo | (tự viết) | `parquet_output/` | Biểu đồ + báo cáo | **Đã làm** |

Sơ đồ luồng dữ liệu:

```
Order_delivery.csv                (dữ liệu gốc, 100.002 dòng)
      │   Bạn 1: frompyspark.py    → đọc, làm sạch
      ▼
data_parquet/clean/                (25 cột)
      │   Bạn 2: transformation.py → thêm cột thời gian, phân loại
      ▼
data_parquet/transformed/        (33 cột)
      │   Bạn 3: aggregation.ipynb → groupBy, thống kê
      ▼
output/*.xlsx                     (10 bảng tổng hợp dạng Excel)

data_parquet/transformed/
      │   Bạn 4: save_parquet.py   → tổng hợp lại 5 bảng, lưu Parquet
      ▼
parquet_output/*                  (5 bảng Parquet)
      │   Bạn 5: vẽ biểu đồ, viết báo cáo
      ▼
Báo cáo cuối
```

> **Lưu ý:** Bạn 3 và bạn 4 **cùng đọc** `data_parquet/transformed/`. Hai bạn **dùng chung** hàm tổng hợp `build_tables()` trong `aggregations.py`, nên số liệu hai nơi luôn khớp nhau.

---

## 3. Cấu trúc thư mục

```
BigdataProject/
├── Order_delivery.csv              # Dữ liệu gốc
├── frompyspark.py                  # Bước 1: làm sạch
├── data_parquet/clean/              # Output bước 1
├── transformation.py               # Bước 2: biến đổi
├── data_parquet/transformed/      # Output bước 2 (33 cột)
├── aggregation.ipynb               # Bước 3: tổng hợp
├── output/                         # Output bước 3 (Excel)
├── save_parquet.py                 # Bước 4: lưu Parquet
├── parquet_output/                 # Output bước 4 (Parquet)
├── requirements.txt                # Danh sách thư viện
├── README.md                       # File này
└── README_SETUP.md                 # Hướng dẫn cài môi trường
```

---

## 4. Chi tiết từng bước

### Bước 1: Đọc & làm sạch (`frompyspark.py`)

**Tạo ra: `data_parquet/clean/`** (25 cột)

- Đọc `Order_delivery.csv` bằng Spark (`header=true`, `inferSchema=true`).
- Bỏ các dòng **trùng hoàn toàn** (`dropDuplicates()`).
- Bỏ các dòng **thiếu `Order_ID` hoặc `User_ID`**.
- Xóa khoảng trắng thừa ở cột `Item_Name` và `City` (`trim()`).
- Tạo cột **`revenue`** (doanh thu) bằng đúng `Total_Price`.
- Bỏ các dòng có `Quantity < 0` hoặc `revenue < 0`.
- Tạo cột **`order_segment`** (phân khúc đơn):
  - `revenue < 200` → **Low**
  - `200 ≤ revenue ≤ 500` → **Medium**
  - `revenue > 500` → **High**

### Bước 2: Biến đổi (`transformation.py`)

**Tạo ra: `data_parquet/transformed/`** (thêm 8 cột, tổng 33 cột)

| Cột mới | Ý nghĩa |
|---|---|
| `Order_Timestamp` | Đổi `Order_Time` (dạng `dd-MM-yyyy HH:mm`) sang kiểu thời gian |
| `Order_Date` | Ngày đặt hàng |
| `Order_Hour` | Giờ đặt hàng (0–23) |
| `Order_Day` | Ngày trong tháng |
| `Order_Month` | Tháng |
| `Delivery_Duration_Hours` | Thời gian giao, đổi từ phút sang giờ (`Delivery_Duration_Minutes / 60`) |
| `Distance_Category` | `< 3 km` → **Near**; `3–5 km` → **Medium**; `> 5 km` → **Far** |
| `Delivery_Category` | `≤ 30 phút` → **Fast**; `≤ 45 phút` → **Normal**; `> 45 phút` → **Slow** |

### Bước 3: Tổng hợp & thống kê (`aggregation.ipynb`)

**Tạo ra: thư mục `output/` gồm 10 file Excel.** Dùng `groupBy().agg()` trên `data_parquet/transformed/`.

| File Excel | Gom theo | Cột chính |
|---|---|---|
| `overview.xlsx` | Toàn bộ dữ liệu | Tổng đơn, tổng doanh thu, doanh thu TB, giờ giao TB, km TB, tổng số lượng |
| `city_summary.xlsx` | `City` | Tổng đơn, tổng doanh thu, doanh thu TB, giờ giao TB |
| `top_city_revenue.xlsx` | Top 10 thành phố | Xếp theo doanh thu |
| `top_city_orders.xlsx` | Top 10 thành phố | Xếp theo số đơn |
| `city_revenue_analysis.xlsx` | `City` | Thêm `Revenue_Share_Percent` (tỷ trọng doanh thu %) |
| `segment_summary.xlsx` | `order_segment` | Tổng đơn, doanh thu, số lượng TB |
| `delivery_summary.xlsx` | `Delivery_Category` | Tổng đơn, giờ giao TB, km TB, doanh thu |
| `distance_summary.xlsx` | `Distance_Category` | Tổng đơn, km TB, giờ giao TB, doanh thu |
| `hour_summary.xlsx` | `Order_Hour` | Tổng đơn, doanh thu, doanh thu TB, giờ giao TB |
| `top_hours.xlsx` | Top 10 giờ | Xếp theo số đơn |

### Bước 4: Lưu vào Parquet (`save_parquet.py`)

**Tạo ra: thư mục `parquet_output/`**

**Vì sao dùng Parquet mà không dùng Excel?** Parquet lưu theo cột, có nén, đọc nhanh và **giữ đúng kiểu dữ liệu**. Excel chậm hơn và không giữ kiểu dữ liệu chặt.

Bước này lưu **5 bảng đã tổng hợp** (dữ liệu chi tiết đã nằm trong `data_parquet/` từ bước 1 và 2). Có **5 bảng**:

```
parquet_output/
├── city_summary/       → part-0.parquet
├── segment_summary/    → part-0.parquet
├── delivery_summary/   → part-0.parquet
├── distance_summary/   → part-0.parquet
└── hour_summary/       → part-0.parquet
```

Các bảng `top_*` không cần lưu riêng vì chỉ là `sort` + lấy 10 dòng đầu của `city_summary` và `hour_summary`.

> Code của nhóm **không dùng Hive**. Nếu thấy file `derby.log` hoặc thư mục `metastore_db`, cứ bỏ qua, đó chỉ là file thừa.

---

## 5. Từ điển dữ liệu (các cột quan trọng)

| Cột | Ý nghĩa |
|---|---|
| `Order_ID`, `User_ID`, `Restaurant_ID`, `Driver_ID` | Mã đơn, khách, nhà hàng, tài xế |
| `Item_Name`, `Quantity`, `Total_Price` | Món, số lượng, tổng giá |
| `Order_Time`, `Delivery_Time` | Thời điểm đặt và giao (chuỗi) |
| `Delivery_Duration_Minutes` | Thời gian giao (phút) |
| `City` | Thành phố |
| `Payment_Method`, `Order_Status` | Hình thức thanh toán, trạng thái đơn |
| `Driver_Vehicle` | Phương tiện (Bicycle, Car...) |
| `Delivery_Distance_km` | Khoảng cách giao (km) |
| `Traffic_Level`, `Driver_Availability` | Mức giao thông, tình trạng tài xế |
| `revenue` | Doanh thu của đơn (bằng `Total_Price`) |
| `order_segment` | Low / Medium / High (bước 1) |
| `Order_Hour`, `Order_Day`, `Order_Month` | Mốc thời gian (bước 2) |
| `Distance_Category`, `Delivery_Category` | Near/Medium/Far, Fast/Normal/Slow (bước 2) |

---

## 6. Việc của bạn 5: Trực quan hóa & báo cáo

### 6.1. Lấy dữ liệu ở đâu?

**Cách 1 (khuyên dùng): đọc Parquet bằng pandas.** Chỉ cần `pandas` và `pyarrow`, **không cần Java hay Spark**.

```python
import pandas as pd

base = "parquet_output"   # đường dẫn tới thư mục parquet_output

city     = pd.read_parquet(f"{base}/city_summary")
segment  = pd.read_parquet(f"{base}/segment_summary")
delivery = pd.read_parquet(f"{base}/delivery_summary")
distance = pd.read_parquet(f"{base}/distance_summary")
hour     = pd.read_parquet(f"{base}/hour_summary").sort_values("Order_Hour")
```

**Cách 2: đọc Excel của bạn 3** (nếu Parquet bị lỗi).

```python
city = pd.read_excel("output/city_summary.xlsx")
```

### 6.2. Các cột trong 5 bảng Parquet

| Bảng | Cột |
|---|---|
| `city_summary` | `City`, `Total_Orders`, `Total_Revenue`, `Average_Revenue`, `Average_Delivery_Hours`, `Revenue_Share_Percent` |
| `segment_summary` | `order_segment`, `Total_Orders`, `Total_Revenue`, `Average_Revenue`, `Average_Quantity` |
| `delivery_summary` | `Delivery_Category`, `Total_Orders`, `Average_Delivery_Hours`, `Average_Distance_km`, `Total_Revenue` |
| `distance_summary` | `Distance_Category`, `Total_Orders`, `Average_Distance_km`, `Average_Delivery_Hours`, `Total_Revenue` |
| `hour_summary` | `Order_Hour`, `Total_Orders`, `Total_Revenue`, `Average_Revenue`, `Average_Delivery_Hours` |

### 6.3. Biểu đồ đề xuất

| Biểu đồ | Bảng | Loại |
|---|---|---|
| Doanh thu theo thành phố | `city_summary` | Cột (bar) |
| Tỷ trọng doanh thu theo thành phố | `city_summary` (`Revenue_Share_Percent`) | Tròn hoặc cột |
| Số đơn theo giờ trong ngày | `hour_summary` | Đường (line) |
| Doanh thu theo phân khúc | `segment_summary` | Tròn hoặc cột |
| Số đơn theo Fast / Normal / Slow | `delivery_summary` | Cột |
| Số đơn theo Near / Medium / Far | `distance_summary` | Cột |
| Thời gian giao theo khoảng cách | `distance_summary` | Cột |

Ví dụ vẽ nhanh:

```python
import matplotlib.pyplot as plt

d = city.sort_values("Total_Revenue", ascending=False)
d.plot.bar(x="City", y="Total_Revenue", legend=False)
plt.title("Doanh thu theo thành phố")
plt.ylabel("Doanh thu")
plt.tight_layout()
plt.show()
```

### 6.4. Gợi ý bố cục báo cáo

1. Giới thiệu bài toán và dữ liệu (100k đơn, 23 cột gốc).
2. Quy trình xử lý 4 bước (dùng sơ đồ ở mục 2).
3. Kết quả: mỗi biểu đồ kèm 1–2 câu nhận xét.
4. Kết luận và hạn chế (xem mục 8).

---

## 7. Kết quả chính (để đối chiếu số liệu)

**Tổng quan**

| Chỉ số | Giá trị |
|---|---|
| Tổng đơn | **100.002** |
| Tổng doanh thu | khoảng **26,89 triệu** |
| Doanh thu TB mỗi đơn | khoảng **268,9** |
| Thời gian giao TB | khoảng **0,625 giờ** (≈ 37,5 phút) |
| Khoảng cách TB | khoảng **2,17 km** |
| Tổng số lượng món | **299.116** |

**Theo phân khúc đơn**

| Phân khúc | Số đơn | Doanh thu |
|---|---|---|
| Medium | 44.838 | khoảng 14,48 triệu |
| High | 12.320 | khoảng 7,38 triệu |
| Low | 42.844 | khoảng 5,03 triệu |

**Theo hiệu suất giao hàng:** Normal 48.411 đơn, Fast 27.414 đơn, Slow 24.177 đơn.

**Theo khoảng cách:** Near 77.069 đơn, Medium 22.779 đơn, Far chỉ **154** đơn.

**Theo thành phố:** doanh thu các thành phố rất đều nhau (mỗi nơi khoảng 14%). Dẫn đầu là Zagazig (khoảng 3,91 triệu, 14,52%), tiếp theo là Tanta, Cairo, Alexandria.

> Nếu số bạn tính ra khác bảng trên, hãy báo nhóm để kiểm tra.

---

## 8. Lưu ý và hạn chế (nên viết vào báo cáo)

- **Dữ liệu có vẻ là dữ liệu mô phỏng:** doanh thu các thành phố gần bằng nhau, thời gian giao gần như không đổi theo khoảng cách (≈ 0,62 giờ ở cả 3 nhóm), số đơn mỗi giờ chênh nhau rất ít (khoảng 4.150–4.260). Vì vậy **tránh kết luận quá mạnh về "quy luật"**.
- Nhóm **Far chỉ có 154 đơn**, nên các số trung bình của nhóm này **kém tin cậy**.
- `Total_Price` được dùng thẳng làm `revenue` và chưa kiểm tra đơn vị tiền tệ.
- Một số số hiển thị dạng khoa học (ví dụ `2.689E7`). Khi vẽ biểu đồ hãy **định dạng lại thành triệu / nghìn** cho dễ đọc.
