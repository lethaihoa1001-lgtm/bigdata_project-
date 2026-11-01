# Hướng dẫn cài môi trường để chạy code (Windows)

Tài liệu này giúp **bất kỳ ai** cài môi trường trên máy mình để dùng code của nhóm.
Giới thiệu dự án và phân công nằm ở file **`README.md`**.

> **Đọc kỹ các ô có chữ ⚠️ và phần in đậm.** Đó là những chỗ hay sai nhất.

---

## 0. Bạn cần cài tới đâu? (CHỌN 1 TRONG 2 ĐƯỜNG)

| | **Đường A: chỉ xem dữ liệu, vẽ biểu đồ** | **Đường B: chạy lại toàn bộ code Spark** |
|---|---|---|
| Dành cho | Người làm biểu đồ, báo cáo (bạn 5) | Người muốn chạy lại `frompyspark.py`, `transformation.py`, `save_parquet.py` |
| Cần Java 17? | **Không** | **Có** |
| Cần PySpark? | **Không** | **Có** |
| Độ khó | Dễ, khoảng 10 phút | Khó hơn, hay gặp lỗi |
| Dữ liệu cần có | Thư mục `parquet_output/` | File `Order_delivery.csv` |

> 👉 Nếu chỉ vẽ biểu đồ, **làm Đường A là đủ**. Dữ liệu đã được xử lý sẵn trong `parquet_output/`, không cần chạy lại Spark. Đường A chạy được thì **dừng lại ở đó**.

---

## Bước 1 (cả 2 đường): Kiểm tra Python

Mở **PowerShell** và gõ:

```powershell
python --version
```

- Phải ra **Python 3.10 trở lên** (nhóm đang dùng 3.13 vẫn chạy tốt).
- Nếu không tìm thấy Python, tải tại **python.org**. Khi cài, **nhớ tích ô "Add Python to PATH"**.
- Ổ đĩa nên còn trống **ít nhất 3 GB** nếu làm Đường B (PySpark nặng khoảng 450 MB, nhưng lúc cài cần chỗ trống gấp nhiều lần).

---

## Bước 2 (cả 2 đường): Đặt thư mục dự án ở chỗ không có khoảng trắng

⚠️ **Đường dẫn KHÔNG ĐƯỢC có khoảng trắng hoặc dấu tiếng Việt.** Đây là nguyên nhân số 1 gây lỗi `JAVA_GATEWAY_EXITED`.

| | Ví dụ |
|---|---|
| ✅ Đúng | `D:\BigdataProject` hoặc `C:\BigdataProject` |
| ❌ Sai | `C:\Users\Nguyen Van A\Desktop\Dự án` (có khoảng trắng và dấu) |

Giải nén hoặc chép toàn bộ file của nhóm vào thư mục đó.

---

## Bước 3 (cả 2 đường): Tạo và bật môi trường ảo (venv)

**venv** là "hộp đồ riêng" chứa Python và thư viện chỉ dành cho dự án này, không làm lộn xộn máy bạn.

```powershell
cd D:\BigdataProject
python -m venv venv
```

(Sửa `D:\BigdataProject` thành thư mục của bạn.) Sẽ xuất hiện thư mục `venv` trong dự án.

Bật venv:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\venv\Scripts\Activate.ps1
```

✅ **Thành công khi thấy chữ `(venv)` ở đầu dòng lệnh.**

> ⚠️ **Mỗi lần mở PowerShell mới, phải bật venv lại** bằng `.\venv\Scripts\Activate.ps1`. Không thấy `(venv)` nghĩa là chưa bật.
>
> **Không** copy thư mục `venv` của người khác sang máy mình. Nó gắn cứng với máy gốc nên sẽ không chạy. Mỗi người **tự tạo** venv.

---

# ĐƯỜNG A: Chỉ xem dữ liệu, vẽ biểu đồ

## A1. Cài thư viện nhẹ

Khi đã thấy `(venv)`:

```powershell
pip install "pandas<3" pyarrow matplotlib openpyxl jupyter ipykernel
```

| Thư viện | Dùng để |
|---|---|
| `pandas` | Đọc và xử lý bảng |
| `pyarrow` | **Đọc file Parquet** (bắt buộc) |
| `matplotlib` | Vẽ biểu đồ |
| `openpyxl` | Đọc file Excel (cách dự phòng) |
| `jupyter`, `ipykernel` | Chạy notebook `.ipynb` |

⚠️ **Gõ đúng `"pandas<3"`** (có dấu ngoặc kép). Pandas 3.x có thể gây cảnh báo.

## A2. Kiểm tra đọc được dữ liệu

Tạo file `test.py` cùng thư mục với `parquet_output`:

```python
import pandas as pd
city = pd.read_parquet("parquet_output/city_summary")
print(city)
```

Chạy:

```powershell
python test.py
```

✅ **Thấy bảng thành phố in ra là xong.** Bạn đã sẵn sàng vẽ biểu đồ. Xem `README.md`, mục 6, để biết cần vẽ biểu đồ nào.

## A3. Nếu dùng VS Code

1. Nhấn `Ctrl+Shift+P`, gõ **`Python: Select Interpreter`**.
2. ⚠️ **Chọn dòng có đuôi `venv\Scripts\python.exe`.**
3. Với notebook (`.ipynb`), chọn **kernel** cùng Python đó (góc phải trên của notebook).

---

# ĐƯỜNG B: Chạy lại code Spark (không bắt buộc với người chỉ vẽ biểu đồ)

## B1. Cài Java 17

PySpark 4.x cần **Java 17 trở lên**. Java 8 sẽ báo lỗi `UnsupportedClassVersionError ... 61.0`.

1. Vào **adoptium.net**, chọn **Temurin 17 (LTS)**, Windows, x64.
2. ⚠️ **Tải bản `.zip`, KHÔNG tải `.msi`** (bản `.msi` hay lỗi quyền, mã 2602/2603).
3. Giải nén vào thư mục **không có khoảng trắng**, ví dụ `C:\Java\jdk-17`.
4. Kiểm tra:
   ```powershell
   C:\Java\jdk-17\bin\java -version
   ```
   Kết quả phải ghi **version "17..."**. (Thay đường dẫn theo nơi bạn giải nén.)

## B2. Cài thư viện đầy đủ

Khi đã thấy `(venv)`:

```powershell
pip install --no-cache-dir -r requirements.txt
```

Chờ vài phút. Kiểm tra bằng `pip list`, phải thấy `pyspark`, `pandas` (bản 2.x), `pyarrow`, `openpyxl`.

> ⚠️ Thư viện tên **`pyspark`** (spark = tia lửa), **không phải `pyshark`** (shark = cá mập, là thư viện bắt gói tin mạng). Rất dễ gõ nhầm.

Nếu báo `No space left on device`, xem bảng "Xử lý lỗi" bên dưới.

## B3. Cấu hình đường dẫn Java (làm một lần trên máy bạn)

File `common.py` (dùng chung cho mọi script và notebook) đọc đường dẫn Java và thư mục tạm từ file **`local_config.py`** của riêng máy bạn.
File này **không đưa lên git** (đã nằm trong `.gitignore`), nên mỗi người một đường dẫn mà không gây xung đột khi dùng git.

1. Chép file mẫu thành file cấu hình thật (cùng thư mục):
   ```powershell
   copy local_config.example.py local_config.py
   ```
2. Mở `local_config.py`, sửa 2 dòng:
   ```python
   JAVA_HOME = r'C:\Java\jdk-17'    # thư mục Java 17 bạn giải nén ở bước B1 (có thư mục con bin)
   TMP_DIR = r'C:\tmp'              # ổ còn trống chỗ (máy không có ổ D thì dùng C)
   ```
3. Tạo sẵn thư mục tạm, ví dụ `mkdir C:\tmp`.

Lưu ý:
- `JAVA_HOME` phải là thư mục **có chứa thư mục con `bin`**; giữ chữ **`r`** trước dấu nháy.
- Không có `local_config.py` thì code dùng đường dẫn mặc định của người viết ban đầu (`D:\DOWNLOAD\...`) và sẽ lỗi trên máy khác.

Khi viết code mới, nên dùng đường dẫn tương đối để chạy được trên mọi máy:

```python
import os
BASE = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(BASE, "Order_delivery.csv")
```

---

## B4. Chạy theo đúng thứ tự

```powershell
cd D:\BigdataProject
.\venv\Scripts\Activate.ps1
python frompyspark.py
python transformation.py
python save_parquet.py
```

| Thứ tự | File | Tạo ra |
|---|---|---|
| 1 | `frompyspark.py` | `data_parquet/clean/` |
| 2 | `transformation.py` | `data_parquet/transformed/` |
| 3 | `aggregation.ipynb` (mở bằng VS Code / Jupyter) | `output/*.xlsx` |
| 4 | `save_parquet.py` | `parquet_output/` |

⚠️ **Phải chạy đúng thứ tự**, vì file sau đọc file do bước trước tạo ra. Chạy `frompyspark.py` ra số bản ghi là thành công.

---

## Xử lý lỗi thường gặp

| Lỗi | Nguyên nhân | Cách sửa |
|---|---|---|
| `No module named 'pyspark'` / `'pandas'` / `'pyarrow'` | **Chưa bật venv**, chưa cài, hoặc cài nhầm `pyshark` | Bật venv (phải thấy `(venv)`), cài lại thư viện. Với VS Code, chọn đúng interpreter |
| `running scripts is disabled` khi bật venv | PowerShell chặn script | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned` rồi bật lại |
| `No space left on device` khi cài | Ổ chứa thư mục tạm hết chỗ (pip cần ~2–3 GB tạm) | `mkdir D:\pip_tmp`, `$env:TEMP="D:\pip_tmp"`, `$env:TMP="D:\pip_tmp"`, rồi chạy lại `pip install` |
| `UnsupportedClassVersionError ... 61.0` | Đang dùng Java 8 | Cài **Java 17** (B1) và sửa `JAVA_HOME` (B3) |
| `JAVA_GATEWAY_EXITED` | Sai đường dẫn Java, hoặc đường dẫn có khoảng trắng | Kiểm tra `JAVA_HOME` ở B3, và chắc chắn thư mục dự án **không có khoảng trắng** |
| `'java' is not recognized` | Chưa trỏ Java | Sửa `JAVA_HOME` trong code (B3), hoặc gõ đường dẫn đầy đủ tới `java.exe` |
| `FileNotFoundError ... Order_delivery.csv` | Thiếu file dữ liệu, hoặc chạy sai thư mục | Đặt file CSV cùng thư mục với file `.py` |
| Lỗi `winutils` / `HADOOP_HOME` khi ghi Parquet | Windows thiếu thành phần Hadoop | Tải `winutils.exe` và `hadoop.dll`, đặt vào `C:\hadoop\bin`, thêm `os.environ['HADOOP_HOME'] = r'C:\hadoop'` vào đầu code. (hàm `write_parquet` trong `common.py` tự chuyển sang ghi bằng pandas và in cảnh báo; để Spark ghi được thì cài winutils theo cột này, hoặc chạy trên Linux/Colab) |
| pandas 3.x báo cảnh báo | PySpark chưa hỗ trợ đầy đủ | `pip install "pandas<3"` |
| Pandas không đọc được Parquet | Thiếu `pyarrow` | `pip install pyarrow` |

---

## Khi gửi code cho bạn khác

- ✅ **Gửi:** các file `.py`, `.ipynb`, `README.md`, `README_SETUP.md`, `requirements.txt`, `Order_delivery.csv`, thư mục `parquet_output/` và `output/`.
- ❌ **KHÔNG gửi:** thư mục **`venv`** (nặng cả GB, copy sang máy khác không chạy), thư mục `metastore_db`, file `derby.log`.

---

## Checklist nhanh

**Đường A (vẽ biểu đồ)**
- [ ] `python --version` ra 3.10 trở lên
- [ ] Thư mục dự án **không có khoảng trắng** trong đường dẫn
- [ ] `python -m venv venv`
- [ ] `.\venv\Scripts\Activate.ps1` và **thấy `(venv)`**
- [ ] `pip install "pandas<3" pyarrow matplotlib openpyxl jupyter ipykernel`
- [ ] Chạy `test.py` đọc được `parquet_output/city_summary`

**Đường B (chạy lại Spark), làm thêm**
- [ ] Cài Java 17 (bản zip) và kiểm tra `java -version`
- [ ] `pip install --no-cache-dir -r requirements.txt`
- [ ] Tạo `local_config.py` từ `local_config.example.py` và sửa `JAVA_HOME`, `TMP_DIR`
- [ ] Chạy đúng thứ tự `frompyspark.py` → `transformation.py` → `aggregation.ipynb` → `save_parquet.py`

## Vài lệnh hữu ích

```powershell
deactivate                                              # Tắt venv
pip list                                                # Xem thư viện đã cài
python -c "import sys; print(sys.executable)"           # Xem đang dùng Python nào (phải có \venv\)
python -c "import pyspark; print(pyspark.__version__)"  # Kiểm tra PySpark (chỉ Đường B)
```
