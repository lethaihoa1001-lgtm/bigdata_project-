# Bài cuối kì Big Data: Pipeline phân tích thương mại điện tử

Báo cáo lý thuyết kèm demo thực hành cho 3 phần của môn học:

| Phần | Nội dung | Công cụ |
|---|---|---|
| 4 | Xử lý dữ liệu: làm sạch, biến đổi, tổng hợp | Spark, Hive |
| 5 | Truy vấn và phân tích | Hive, Impala |
| 6 | Điều phối, hỗ trợ | ZooKeeper |

Dữ liệu xuyên suốt: **Olist Brazilian E-Commerce** (~100k đơn hàng, 2016 đến 2018).
Nguồn: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Thành viên

| TV | Họ tên | GitHub | Phụ trách |
|---|---|---|---|
| TV1 | ... | @... | Trưởng nhóm, hạ tầng, chương 00 và 07 |
| TV2 | ... | @... | Phần 4: Spark |
| TV3 | ... | @... | Phần 4: Hive, benchmark Spark vs Hive |
| TV4 | ... | @... | Phần 5: Hive, Impala |
| TV5 | ... | @... | Phần 6: ZooKeeper, sơ đồ kiến trúc |

## Cấu trúc repo

```
data/          dữ liệu (git bỏ qua, chỉ giữ SCHEMA.md)
report/        báo cáo Markdown, mỗi chương một file
demo/          code demo theo từng phần
docs/          sơ đồ, biên bản họp
scripts/       tải dữ liệu, tạo issue
docker-compose.yml
```

## Bắt đầu nhanh

```bash
git clone <url-repo> && cd bigdata-final

# 1. Tải dữ liệu vào data/raw (cần Kaggle CLI, hoặc tải tay rồi giải nén vào data/raw/)
make data

# 2. Dựng môi trường
make up

# 3. Chạy pipeline phần 4 với dữ liệu gốc (1x)
docker compose exec spark /opt/spark/bin/spark-submit demo/04-spark-hive/spark/02_clean.py --scale 1
docker compose exec spark /opt/spark/bin/spark-submit demo/04-spark-hive/spark/03_transform.py --scale 1
docker compose exec spark /opt/spark/bin/spark-submit demo/04-spark-hive/spark/04_aggregate.py --scale 1
```

Chi tiết chạy từng bước và nhân bản dữ liệu: xem `demo/04-spark-hive/README.md`.
Hợp đồng dữ liệu giữa các phần: `data/SCHEMA.md`.

## Cách làm việc nhóm

Xem `CONTRIBUTING.md`. Tóm tắt: không push thẳng vào `main`, mỗi việc một nhánh `feature/...`,
mở Pull Request và nhờ một bạn review.

## Gộp báo cáo

```bash
make pdf   # cần pandoc và xelatex
```
