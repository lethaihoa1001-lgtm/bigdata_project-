"""Cấu hình dùng chung cho mọi script và notebook: môi trường Java, SparkSession, đường dẫn, hàm ghi Parquet.

LUÔN import file này TRƯỚC khi tạo SparkSession.
Đường dẫn Java của từng máy nằm trong local_config.py (không đưa lên git, xem local_config.example.py).
Trên Linux/Colab không cần cấu hình gì: dùng Java có sẵn của hệ thống.
"""
import os
import shutil
import sys
from pathlib import Path

try:
    from local_config import JAVA_HOME, TMP_DIR
except ImportError:  # mặc định: máy của người viết ban đầu
    JAVA_HOME = r'D:\DOWNLOAD\OpenJDK17U-jdk_x64_windows_hotspot_17.0.20.1_1\jdk-17.0.20.1+1'
    TMP_DIR = r'D:\pip_tmp'
try:  # tùy chọn: local_config.py có thể khai báo HADOOP_HOME riêng
    from local_config import HADOOP_HOME
except ImportError:
    HADOOP_HOME = r'D:\DOWNLOAD\winutils-master\winutils-master\hadoop-3.4.0-win10-x64'

if os.name == "nt":  # chỉ cần chỉnh trên Windows
    os.makedirs(TMP_DIR, exist_ok=True)
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = JAVA_HOME + r"\bin;" + os.environ["PATH"]
    os.environ["TEMP"] = TMP_DIR
    os.environ["TMP"] = TMP_DIR
    if os.path.isdir(os.path.join(HADOOP_HOME, "bin")):
        os.environ["HADOOP_HOME"] = HADOOP_HOME
        os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ["PATH"]
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE = Path(__file__).resolve().parent
RAW_CSV = BASE / "Order_delivery.csv"                 # dữ liệu gốc (tải từ Kaggle)
DATA_DIR = BASE / "data_parquet"                      # kho dữ liệu chi tiết, định dạng Parquet
CLEAN_DIR = DATA_DIR / "clean"                        # output bước 1
TRANSFORMED_DIR = DATA_DIR / "transformed"            # output bước 2 (phân vùng theo Order_Month)
PARQUET_OUTPUT = BASE / "parquet_output"              # 5 bảng tổng hợp (cho vẽ biểu đồ)
REPORT_DIR = BASE / "reports"                         # số liệu kiểm tra, benchmark


def get_spark(app_name):
    from pyspark.sql import SparkSession  # import muộn để môi trường Java đã được thiết lập ở trên

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
    )
    if os.name == "nt" and os.path.isdir(os.path.join(HADOOP_HOME, "bin")):
        builder = builder.config("spark.driver.extraJavaOptions",
                                 f"-Dhadoop.home.dir={HADOOP_HOME.replace(chr(92), '/')}")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_parquet(df, path, partition_by=None, num_files=None):
    """Ghi DataFrame ra Parquet BẰNG SPARK (ghi phân tán, không gom về một máy).

    Trên Windows, Spark có thể không ghi được file local nếu thiếu winutils.exe/hadoop.dll.
    Khi đó hàm tự chuyển sang ghi bằng pandas (chỉ nên dùng với dữ liệu nhỏ) và in cảnh báo.
    Trả về "spark" hoặc "pandas" để ghi vào báo cáo.
    """
    path = str(path)
    out = df
    if partition_by:
        out = out.repartition(*partition_by)  # mỗi phân vùng gom vào ít file
    elif num_files:
        out = out.coalesce(num_files)
    writer = out.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    try:
        writer.parquet(path)
        print(f"[OK] Spark đã ghi Parquet: {path}")
        return "spark"
    except Exception as e:  # noqa: BLE001
        print("[CẢNH BÁO] Spark không ghi được Parquet:", str(e).strip().splitlines()[0][:160])
        print("           (Windows thiếu winutils/hadoop.dll?) Chuyển sang ghi bằng pandas; xem README_SETUP.md.")
        pdf = df.toPandas()
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)
        if partition_by:
            pdf.to_parquet(path, partition_cols=list(partition_by), index=False)
        else:
            pdf.to_parquet(os.path.join(path, "part-0.parquet"), index=False)
        print(f"[OK] pandas đã ghi Parquet: {path}")
        return "pandas"


def dir_size_mb(path):
    total = 0
    for root, _, files in os.walk(str(path)):
        for f in files:
            if f.startswith(".") or f.startswith("_"):  # .crc, _SUCCESS
                continue
            total += os.path.getsize(os.path.join(root, f))
    return total / 1024 / 1024
