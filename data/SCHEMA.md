# Hợp đồng dữ liệu (SCHEMA)

Cả nhóm cùng dựa vào file này. Muốn đổi cột hay kiểu dữ liệu: mở PR và báo cả nhóm.

## 1. Dữ liệu gốc `data/raw/` (Olist, 8 bảng dùng + geolocation)

| File | Khóa | Cột chính |
|---|---|---|
| `olist_orders_dataset.csv` | order_id | customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date |
| `olist_order_items_dataset.csv` | (order_id, order_item_id) | product_id, seller_id, shipping_limit_date, price, freight_value |
| `olist_order_payments_dataset.csv` | (order_id, payment_sequential) | payment_type, payment_installments, payment_value |
| `olist_order_reviews_dataset.csv` | (review_id, order_id) | review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp |
| `olist_customers_dataset.csv` | customer_id | customer_unique_id, customer_zip_code_prefix, customer_city, customer_state |
| `olist_products_dataset.csv` | product_id | product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm |
| `olist_sellers_dataset.csv` | seller_id | seller_zip_code_prefix, seller_city, seller_state |
| `product_category_name_translation.csv` | product_category_name | product_category_name_english |
| `olist_geolocation_dataset.csv` | (không có) | **Không dùng** trong pipeline |

Lưu ý: `customer_id` chỉ gắn với một đơn; `customer_unique_id` mới là khách thật (dùng đếm khách mua lặp lại).
Tên cột gốc có lỗi chính tả (`lenght`), được sửa thành `length` ở lớp interim.

## 2. Các mức dữ liệu

| Tag | Nguồn | Thư mục raw | Dùng cho |
|---|---|---|---|
| `1x` | dữ liệu gốc | `data/raw/` | phát triển, demo nhanh |
| `10x` | nhân bản | `data/raw_scaled/10x/` | benchmark |
| `50x` | nhân bản | `data/raw_scaled/50x/` | benchmark |

Dữ liệu nhân bản là **dữ liệu nhân tạo** (đổi hậu tố ID, dịch ngày, chèn dữ liệu bẩn). Báo cáo phải ghi rõ điều này.

## 3. Lớp interim `data/interim/<tag>/<bảng>/` (Parquet, đã làm sạch)

Bảng: `orders`, `order_items`, `order_payments`, `order_reviews`, `customers`, `products`, `sellers`, `category_translation`.
Ngày giờ là `timestamp`, tiền là `double`, số lượng là `int`. Bảng `order_reviews` bỏ hai cột nhận xét dạng văn bản.

Quy tắc làm sạch chính:
- Bỏ dòng thiếu khóa, bỏ trùng theo khóa.
- `orders`: bỏ đơn `delivered` nhưng thiếu ngày giao, bỏ đơn có ngày giao trước ngày mua.
- `order_items`: bỏ `price <= 0` hoặc `freight_value < 0`.
- `order_payments`: bỏ `payment_value < 0` và `payment_type = 'not_defined'`.
- `order_reviews`: chỉ giữ điểm từ 1 đến 5.
- `customers`, `sellers`: chuẩn hóa thành phố (chữ thường, bỏ khoảng trắng thừa), bang viết hoa.

## 4. Lớp processed `data/processed/<tag>/` (Parquet, TV3 và TV4 dùng)

### `fact_orders` (mỗi dòng một đơn) — partition: `order_year`, `order_month`
| Cột | Kiểu | Ghi chú |
|---|---|---|
| order_id, customer_id, customer_unique_id | string | |
| customer_city, customer_state | string | |
| order_status | string | delivered, shipped, canceled... |
| order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date | timestamp | |
| items_amount, freight_amount | double | tổng giá hàng, tổng phí vận chuyển |
| item_count | bigint | số món |
| total_amount | double | items_amount + freight_amount |
| payment_total | double | tổng thanh toán |
| max_installments | int | |
| payment_type_main | string | hình thức có giá trị lớn nhất trong đơn |
| delivery_days | double | ngày giao thực tế (null nếu chưa giao) |
| delay_days | double | giao so với dự kiến (dương là trễ) |
| is_late | int | 1 trễ, 0 đúng hạn, null chưa giao |
| review_score | int | điểm của review mới nhất, 1 đến 5 |
| **order_year, order_month** | int | cột partition |

### `fact_order_items` (mỗi dòng một món) — partition: `order_year`, `order_month`
order_id, order_item_id (int), product_id, seller_id, shipping_limit_date (timestamp), price (double), freight_value (double), category_en, seller_state, order_status, order_year, order_month.

### Bảng chiều (không partition)
- `dim_customers`: customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
- `dim_sellers`: seller_id, seller_zip_code_prefix, seller_city, seller_state
- `dim_products`: product_id, product_category_name, category_en, product_name_length, product_description_length, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm

### Bảng tổng hợp
`agg_revenue_by_month`, `agg_revenue_by_state`, `agg_revenue_by_category`, `agg_payment_type`.
Chỉ tính các đơn có `order_status` khác `canceled` và `unavailable`.

## 5. Lưu ý cho Hive/Impala
- Spark ghi partition theo thư mục `order_year=YYYY/order_month=M/`. Khi tạo bảng external phải khai `PARTITIONED BY (order_year INT, order_month INT)` và chạy `MSCK REPAIR TABLE` (Hive) hoặc `ALTER TABLE ... RECOVER PARTITIONS` (Impala).
- Timestamp ghi theo mặc định của Spark (INT96), Hive và Impala đều đọc được.
- Đường dẫn trong container Hive: `/data/processed/<tag>/<bảng>`.
