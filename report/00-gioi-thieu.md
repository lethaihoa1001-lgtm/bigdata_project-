# Chương 0. Giới thiệu

## 0.1 Bối cảnh và mục tiêu
Các hệ thống thương mại điện tử sinh ra lượng lớn dữ liệu đơn hàng, thanh toán, đánh giá. Bài làm xây dựng
một pipeline nhỏ trên hệ sinh thái Hadoop để minh họa ba vai trò trong xử lý dữ liệu lớn:

1. **Xử lý dữ liệu** (Spark, Hive): làm sạch, biến đổi, tổng hợp.
2. **Truy vấn và phân tích** (Hive, Impala): trả lời các câu hỏi nghiệp vụ và so sánh hiệu năng.
3. **Điều phối, hỗ trợ** (ZooKeeper): giữ cho hệ thống phân tán hoạt động ổn định.

## 0.2 Dữ liệu
Bộ **Brazilian E-Commerce Public Dataset by Olist** (Kaggle): khoảng 100 nghìn đơn hàng từ 2016 đến 2018,
gồm đơn hàng, món hàng, thanh toán, đánh giá, khách hàng, sản phẩm, người bán.
Nguồn: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Để benchmark có ý nghĩa, dữ liệu gốc (1x) được **nhân bản nhân tạo** lên 10x và 50x
(đổi hậu tố ID, dịch mốc thời gian, chèn khoảng 1% dữ liệu bẩn). Kết quả trên các mức nhân bản
chỉ phản ánh hiệu năng, không phản ánh xu hướng kinh doanh thật.

## 0.3 Kiến trúc tổng thể
<!-- TV5: chèn sơ đồ vào report/images/kien-truc-tong-the.png -->
![Kiến trúc tổng thể](images/kien-truc-tong-the.png)

Luồng dữ liệu: `CSV thô → Spark (làm sạch, biến đổi) → Parquet phân vùng → Hive/Impala (truy vấn)`;
ZooKeeper điều phối các thành phần phân tán (ví dụ HiveServer2 đăng ký dịch vụ, NameNode HA).

## 0.4 Môi trường thực nghiệm
<!-- TV1: điền cấu hình máy thật -->
| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Máy chạy | ... CPU, ... GB RAM | |
| Spark | 3.5.1 | docker image `apache/spark:3.5.1-python3` |
| Hive | 4.0.0 | docker image `apache/hive:4.0.0` |
| Impala | ... | |
| ZooKeeper | 3.9.2 | ensemble 3 node |

## 0.5 Phân công
Xem bảng thành viên trong `README.md`; lịch sử đóng góp xem ở lịch sử commit của repo.

## 0.6 Bố cục báo cáo
Chương 4: xử lý dữ liệu. Chương 5: truy vấn và phân tích. Chương 6: ZooKeeper.
Chương 7: so sánh và kết luận.
