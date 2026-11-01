# Nâng cấp phần lưu trữ và xử lý (khía cạnh Big Data)

## Vấn đề của bản cũ
| # | Bản cũ | Vì sao yếu |
|---|---|---|
| 1 | Mỗi bước kết thúc bằng `toPandas()` rồi ghi CSV | Gom toàn bộ dữ liệu về RAM của một máy, bỏ qua khả năng phân tán của Spark |
| 2 | Truyền dữ liệu giữa các bước bằng CSV | CSV không lưu kiểu dữ liệu, mỗi bước phải suy luận lại (`inferSchema`), chậm và dễ sai |
| 3 | Parquet chỉ chứa 5 bảng tổng hợp, do pandas ghi | Dữ liệu chi tiết không nằm trong kho Parquet; không phải Spark ghi |
| 4 | Phép tổng hợp viết hai lần (notebook và `save_parquet.py`) | Hai nguồn sự thật, dễ lệch (một nơi làm tròn số, một nơi không) |

## Bản mới
```
Order_delivery.csv
   │ frompyspark.py     Spark đọc, làm sạch, GHI PARQUET
   ▼
data_parquet/clean/
   │ transformation.py  Spark đọc Parquet, biến đổi, GHI PARQUET PHÂN VÙNG theo Order_Month
   ▼
data_parquet/transformed/Order_Month=1/ ... Order_Month=12/
   │ aggregations.py (build_tables): MỘT nơi định nghĩa phép tổng hợp
   ├── aggregation.ipynb → output/*.xlsx (10 bảng, kết quả nhỏ)
   └── save_parquet.py   → parquet_output/ (5 bảng) → draw_charts.py
```

| # | Thay đổi | File |
|---|---|---|
| 1 | Không dùng `toPandas()` cho dữ liệu chi tiết; chỉ dùng cho bảng tổng hợp vài chục dòng (xuất Excel) | tất cả |
| 2 | Parquet thay CSV giữa các bước: giữ đúng kiểu dữ liệu, lưu theo cột, có nén | `frompyspark.py`, `transformation.py` |
| 3 | Dữ liệu chi tiết lưu ở `data_parquet/`, **phân vùng theo `Order_Month`**; Spark là bên ghi | `common.write_parquet` |
| 4 | Phép tổng hợp gom về `aggregations.build_tables()`; notebook và `save_parquet.py` cùng gọi | `aggregations.py` |
| 5 | Thêm `storage_demo.py` đo dung lượng, thời gian CSV so với Parquet, hiển thị partition pruning | `storage_demo.py` |
| 6 | Thêm `check_results.py` đối chiếu kết quả với số liệu trong README | `check_results.py` |

**Logic làm sạch và biến đổi giữ nguyên** so với bản cũ, nên kết quả phải giống mục 7 của `README.md`.
Notebook đã được xóa kết quả cũ; chạy lại để sinh kết quả mới.

## Cách chạy (thứ tự)
```
python frompyspark.py       # -> data_parquet/clean/, reports/cleaning_report.json
python transformation.py    # -> data_parquet/transformed/
python save_parquet.py      # -> parquet_output/
python check_results.py     # đối chiếu với README (phải in "Tất cả khớp")
python storage_demo.py      # -> reports/storage_benchmark.csv (số liệu đưa vào báo cáo)
```
`aggregation.ipynb` chạy sau `transformation.py` để xuất Excel. Cài Java và `local_config.py` theo `README_SETUP.md`.

## Lưu ý về Windows
Spark trên Windows ghi file local cần `winutils.exe` và `hadoop.dll`. Nếu thiếu, `write_parquet` tự chuyển sang ghi bằng pandas và in dòng `[CẢNH BÁO]`.
Khi đó dữ liệu vẫn đúng nhưng **không phải Spark ghi**: ghi trung thực trong báo cáo, hoặc chạy trên Colab/Codespaces (Linux), nơi Spark ghi bình thường.
Dòng cuối của `reports/cleaning_report.json` (`parquet_writer`) ghi lại bên nào đã ghi.

## Gợi ý viết vào báo cáo
- Lý do dùng Parquet: lưu theo cột, nén, giữ kiểu dữ liệu, chỉ đọc các cột cần dùng. Kèm số liệu dung lượng và thời gian từ `reports/storage_benchmark.csv`.
- Lý do phân vùng theo tháng: truy vấn lọc theo tháng chỉ đọc đúng thư mục (xem `PartitionFilters` trong kết quả `storage_demo.py`).
- Hạn chế cần nêu: dữ liệu chỉ khoảng 100 nghìn dòng và Spark chạy chế độ local trên một máy, nên lợi ích của phân vùng và Parquet chủ yếu mang tính minh họa; với dữ liệu nhỏ, CSV và Parquet có thể chênh nhau ít.
