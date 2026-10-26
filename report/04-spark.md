# Chương 4A. Xử lý dữ liệu với Apache Spark

## 4A.1 Tổng quan
Apache Spark là engine xử lý dữ liệu phân tán, giữ dữ liệu trung gian trong bộ nhớ khi có thể và biểu diễn
tính toán dưới dạng đồ thị có hướng không chu trình (DAG). Spark chạy được trên YARN, Kubernetes hoặc chế độ độc lập,
đọc dữ liệu từ HDFS, S3 hay hệ thống file cục bộ.

## 4A.2 Kiến trúc
- **Driver**: chạy chương trình chính, tạo SparkSession, lập kế hoạch và chia job thành stage, task.
- **Cluster manager**: cấp phát tài nguyên (YARN, Kubernetes, standalone).
- **Executor**: tiến trình trên worker, chạy task và giữ dữ liệu cache.
- **Job, stage, task**: một *action* (như `write`, `count`) sinh một job; job chia thành các stage tại ranh giới shuffle;
  mỗi stage gồm nhiều task chạy song song trên các partition.

<!-- TODO(TV2): chèn ảnh chụp Spark UI (tab Jobs và DAG) vào report/images/p4-spark-ui-dag.png -->

## 4A.3 Mô hình dữ liệu và cơ chế thực thi
- **RDD**: tập dữ liệu phân tán bất biến, mức thấp.
- **DataFrame/Dataset**: dữ liệu có schema, tối ưu bởi **Catalyst** (đẩy bộ lọc xuống sớm, chọn cột cần thiết, sắp xếp lại join)
  và **Tungsten** (quản lý bộ nhớ, sinh mã).
- **Lazy evaluation**: *transformation* (`select`, `filter`, `join`) chỉ xây kế hoạch; chỉ *action* mới kích hoạt tính toán.
- **Narrow và wide transformation**: `filter`, `select` không cần shuffle; `groupBy`, `join` thường cần shuffle dữ liệu qua mạng.
- **Chịu lỗi**: mất partition thì tính lại từ lineage thay vì sao lưu mọi bước trung gian.

## 4A.4 So sánh Spark và Hadoop MapReduce
| Tiêu chí | MapReduce | Spark |
|---|---|---|
| Dữ liệu trung gian | ghi ra đĩa (HDFS) sau mỗi giai đoạn | giữ trong bộ nhớ, tràn ra đĩa khi cần |
| Mô hình | map rồi reduce, tác vụ phức tạp phải nối nhiều job | DAG nhiều bước trong một job |
| Thuật toán lặp, tương tác | chậm vì đọc ghi đĩa lặp lại | phù hợp nhờ cache |
| API | Java, mức thấp | Python, Scala, Java, R, SQL, DataFrame |
| Xử lý luồng, ML, đồ thị | cần công cụ ngoài | có Structured Streaming, MLlib, GraphX |
| Độ trễ | cao | thấp hơn đáng kể với nhiều workload |
| Tiêu thụ bộ nhớ | thấp | cao hơn, cần cấu hình hợp lý |

Ghi chú: mức nhanh hơn phụ thuộc workload; con số "nhanh hơn gấp nhiều lần" cần dẫn nguồn nếu đưa vào báo cáo.
<!-- TODO(TV2): dẫn nguồn (tài liệu Apache Spark hoặc bài báo) cho nhận định về hiệu năng -->

## 4A.5 Demo: pipeline xử lý dữ liệu Olist
Mã nguồn: `demo/04-spark-hive/spark/`. Hướng dẫn chạy: `demo/04-spark-hive/README.md`.

### Bước 1. Nhân bản dữ liệu (`01_scale_data.py`)
Nhân bản dữ liệu gốc lên 10x và 50x bằng `crossJoin` với bảng chỉ số bản sao; bản sao thứ k đổi hậu tố ID
và dịch mốc thời gian k × 30 ngày. Chèn khoảng 1% dữ liệu bẩn: dòng trùng đơn, đơn `delivered` mất ngày giao,
giá âm, tên thành phố sai định dạng.

### Bước 2. Làm sạch (`02_clean.py`)
Quy tắc: xem `data/SCHEMA.md` mục 3. Kết quả số dòng trước và sau:

<!-- TODO(TV2): dán bảng từ data/interim/<tag>/_cleaning_metrics.json -->
| Bảng | Trước | Sau | Loại bỏ |
|---|---|---|---|
| orders | | | |
| order_items | | | |
| order_payments | | | |
| order_reviews | | | |
| customers | | | |

### Bước 3. Biến đổi (`03_transform.py`)
- Gộp món hàng, thanh toán, đánh giá về cấp đơn bằng `groupBy` và hàm cửa sổ (`row_number`) để chọn hình thức thanh toán chính và đánh giá mới nhất.
- Tính cột mới: `total_amount`, `delivery_days`, `delay_days`, `is_late`, `order_year`, `order_month`.
- Ghi `fact_orders` và `fact_order_items` ở dạng Parquet phân vùng theo `order_year`, `order_month`; ghi các bảng chiều.

Lý do chọn Parquet: lưu theo cột, nén tốt, đọc được bởi Hive và Impala; phân vùng giúp truy vấn theo thời gian chỉ đọc đúng thư mục cần.

### Bước 4. Tổng hợp (`04_aggregate.py`)
Doanh thu theo tháng, theo bang, theo danh mục và theo hình thức thanh toán (loại đơn `canceled`, `unavailable`).

<!-- TODO(TV2): chèn ảnh kết quả vào report/images/p4-agg-*.png và nhận xét 2 đến 3 câu cho mỗi bảng -->

## 4A.6 Kết quả hiệu năng
Thời gian lấy từ `demo/04-spark-hive/results/spark_timings.csv`.

<!-- TODO(TV2): điền bảng; TV3 bổ sung cột Hive ở chương 4B -->
| Bước | 1x (s) | 10x (s) | 50x (s) |
|---|---|---|---|
| clean (tổng) | | | |
| transform_fact_orders | | | |
| transform_fact_order_items | | | |
| aggregate (tổng) | | | |

Nhận xét: thời gian tăng theo kích thước dữ liệu thế nào, bước nào là điểm nghẽn (thường là shuffle ở `join` và `groupBy`).

## 4A.7 Hạn chế
- Dữ liệu nhân bản nhân tạo nên phân phối giữa các bản sao giống nhau.
- Chạy trên một máy (chế độ local trong container), chưa phản ánh hiệu năng cụm nhiều node.

## Tài liệu tham khảo
<!-- TODO(TV2): Zaharia et al., "Resilient Distributed Datasets" (NSDI 2012); tài liệu Apache Spark; Olist dataset trên Kaggle -->
