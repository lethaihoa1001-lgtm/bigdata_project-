# Quy ước làm việc nhóm

## Nhánh
- `main`: bản ổn định, không ai push trực tiếp (bật branch protection: yêu cầu PR và 1 approval).
- Mỗi việc một nhánh: `feature/<phan>-<noi-dung>`, ví dụ `feature/p4-spark-cleaning`.
- Cập nhật trước khi làm: `git checkout main && git pull`.

## Commit
Dạng `loai(phan): mô tả ngắn`, commit nhỏ và thường xuyên.

| Loại | Dùng khi |
|---|---|
| `docs` | viết hoặc sửa báo cáo |
| `feat` | thêm script/demo mới |
| `fix` | sửa lỗi |
| `test` | benchmark, kiểm thử |
| `chore` | cấu hình, docker, dọn dẹp |

Ví dụ: `docs(p4): viết phần kiến trúc Spark`, `feat(p4): script làm sạch bảng orders`.

## Pull Request
1. Push nhánh, mở PR vào `main`, điền template.
2. Ghi `Closes #<số issue>` để issue tự đóng khi merge.
3. Cần ít nhất 1 approval rồi mới merge (chọn "Squash and merge" hoặc "Merge commit", cả nhóm thống nhất một kiểu).

## Ai review cho ai
| Người viết | Người review |
|---|---|
| TV2 | TV3 |
| TV3 | TV2 |
| TV4 | TV5 |
| TV5 | TV4 |
| TV1 | bất kỳ bạn nào |
| Bản cuối lên `main` | TV1 kiểm tra lần cuối |

## Theo dõi tiến trình
- Việc cần làm nằm ở **Issues**, gán người và milestone theo tuần.
- Bảng **Projects** (To do, In progress, Review, Done).
- Họp ngắn đầu tuần, ghi biên bản vào `docs/meeting-notes/YYYY-MM-DD.md`.
- Xem đóng góp: tab Insights → Contributors, hoặc `git shortlog -sn`.

## Quy tắc khác
- Không commit dữ liệu lớn, mật khẩu, token.
- Ảnh kết quả đặt trong `report/images/` với tên rõ nghĩa (`p5-benchmark-hive-vs-impala.png`).
- Báo cáo viết bằng Markdown, mỗi người chỉ sửa file chương của mình để tránh conflict.
