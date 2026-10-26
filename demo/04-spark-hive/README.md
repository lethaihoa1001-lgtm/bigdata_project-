# Demo phần 4: Spark (TV2) và Hive (TV3)

## Luồng chạy

```
data/raw  ──01_scale_data (tùy chọn)──►  data/raw_scaled/<N>x
   │                                          │
   └──────────────02_clean────────────────────┘──►  data/interim/<tag>
                                                         │ 03_transform
                                                         ▼
                                        data/processed/<tag>  ──04_aggregate──► agg_*
```

`<tag>` là `1x` (dữ liệu gốc), `10x`, `50x`. Mọi script nhận `--scale N` (mặc định 1).

## Chạy bằng Docker (khuyến nghị cho cả nhóm)

```bash
docker compose up -d spark
SUBMIT="docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 4g"
D=demo/04-spark-hive/spark

# 1x
$SUBMIT $D/02_clean.py     --scale 1
$SUBMIT $D/03_transform.py --scale 1
$SUBMIT $D/04_aggregate.py --scale 1

# 10x và 50x: nhân bản trước rồi chạy lại ba bước với --scale tương ứng
$SUBMIT $D/01_scale_data.py --factor 10
$SUBMIT $D/02_clean.py      --scale 10
```

Spark UI: http://localhost:4040 khi job đang chạy (chụp ảnh DAG, stage đưa vào báo cáo).

## Chạy trực tiếp trên máy (không Docker)

Cần Java 17 và Python 3.9+:

```bash
pip install pyspark==3.5.1
cd demo/04-spark-hive/spark
python 02_clean.py --scale 1
```

## Kết quả sinh ra
- `data/interim/<tag>/_cleaning_metrics.json`: số dòng trước/sau làm sạch từng bảng (đưa vào báo cáo).
- `demo/04-spark-hive/results/spark_timings.csv`: thời gian từng bước, commit file này làm bằng chứng benchmark.
- In ra console các bảng `agg_*` để chụp ảnh.

## Bàn giao cho TV3 và TV4
Đọc `data/processed/<tag>/` theo hợp đồng ở `data/SCHEMA.md`. TV3 đặt file HiveQL ở `hive/`,
dùng cùng tác vụ (làm sạch, tổng hợp doanh thu theo tháng) để so sánh thời gian với `spark_timings.csv`.

## Điều cần kiểm tra khi chạy lần đầu
Các script chưa được chạy thử trên dữ liệu thật. Người chạy đầu tiên (TV2) kiểm tra:
- Số dòng sau làm sạch hợp lý (với 1x, bảng orders mất rất ít dòng).
- `fact_orders` có `order_year`, `order_month` đúng và không có partition lạ (năm null).
- Nếu thiếu RAM ở 50x: tăng `--driver-memory` hoặc đặt `SHUFFLE_PARTITIONS` lớn hơn.
