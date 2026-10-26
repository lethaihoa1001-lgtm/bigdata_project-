#!/usr/bin/env bash
# Tạo nhãn, milestone và issue khởi tạo.
# Yêu cầu: GitHub CLI đã đăng nhập (gh auth login), chạy trong repo đã push lên GitHub.
set -euo pipefail

for l in tv1 tv2 tv3 tv4 tv5; do gh label create "$l" --force >/dev/null; done
M1="Tuần 1 - Môi trường và dữ liệu"; M2="Tuần 2 - Demo"; M3="Tuần 3 - Báo cáo và review"
for m in "$M1" "$M2" "$M3"; do gh api repos/:owner/:repo/milestones -f title="$m" >/dev/null || true; done

issue() { gh issue create --title "$1" --body "$2" --milestone "$3" --label "$4"; }

issue "[TV1] Dựng docker-compose (Spark, Hive, ZooKeeper)" "Chạy được docker compose up -d, ghi cách dùng vào README." "$M1" tv1
issue "[TV1] Chốt dataset và hoàn thiện data/SCHEMA.md" "Cả nhóm review hợp đồng dữ liệu." "$M1" tv1
issue "[TV1] Viết report/00-gioi-thieu.md" "Bài toán, dữ liệu, kiến trúc tổng thể." "$M2" tv1
issue "[TV1] Viết report/07-so-sanh-ket-luan.md" "Tổng hợp benchmark Spark/Hive/Impala, kết luận." "$M3" tv1
issue "[TV1] Gộp báo cáo PDF và review bản cuối" "make pdf, kiểm tra mục lục, ảnh, trích dẫn." "$M3" tv1

issue "[TV2] Script nhân bản dữ liệu 10x và 50x" "demo/04-spark-hive/spark/01_scale_data.py" "$M1" tv2
issue "[TV2] Script làm sạch (Spark)" "demo/04-spark-hive/spark/02_clean.py, ghi số dòng trước/sau." "$M2" tv2
issue "[TV2] Script biến đổi, tạo fact_orders và fact_order_items" "demo/04-spark-hive/spark/03_transform.py, ghi Parquet partition." "$M2" tv2
issue "[TV2] Script tổng hợp và đo thời gian" "demo/04-spark-hive/spark/04_aggregate.py, kết quả ở results/spark_timings.csv." "$M2" tv2
issue "[TV2] Viết report/04-spark.md" "Lý thuyết Spark vs MapReduce và kết quả demo." "$M3" tv2

issue "[TV3] Tạo bảng external và partition trên Hive" "Trỏ vào data/processed/<tag>/." "$M2" tv3
issue "[TV3] Benchmark Spark vs Hive cùng một tác vụ" "Dùng results/spark_timings.csv của TV2." "$M2" tv3
issue "[TV4] Viết 7 câu truy vấn nghiệp vụ" "Chạy trên Hive và Impala." "$M2" tv4
issue "[TV4] Benchmark Hive vs Impala và vẽ biểu đồ" "3 mức dữ liệu: 1x, 10x, 50x." "$M2" tv4
issue "[TV5] Demo zkCli và leader election" "Ensemble 3 node trong docker-compose." "$M2" tv5
issue "[TV5] Vẽ sơ đồ kiến trúc tổng thể" "Lưu vào docs/ và report/images/." "$M1" tv5
