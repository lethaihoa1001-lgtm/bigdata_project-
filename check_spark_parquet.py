"""Kiểm tra Spark có ghi/đọc Parquet NATIVE (không qua pandas) được không.

Chạy:  python check_spark_parquet.py
Kết quả mong muốn: in ra "THANH CONG".
"""
import glob
import os
import shutil

from common import HADOOP_HOME, get_spark  # import trước pyspark

import pyspark

jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api*.jar"))
print("PySpark:", pyspark.__version__)
print("Hadoop trong PySpark:", [os.path.basename(j) for j in jars])
print("HADOOP_HOME:", HADOOP_HOME)
for name in ("winutils.exe", "hadoop.dll"):
    p = os.path.join(HADOOP_HOME, "bin", name)
    print(f"  {name}:", "CO" if os.path.exists(p) else "THIEU  <-- can dat vao " + os.path.dirname(p))

spark = get_spark("check_parquet")
out = os.path.join(os.environ.get("TEMP", "."), "check_parquet_test")
shutil.rmtree(out, ignore_errors=True)

df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "v"])
df.write.mode("overwrite").parquet(out)          # ghi bang Spark
n = spark.read.parquet(out).count()              # doc bang Spark
print("THANH CONG: Spark ghi + doc Parquet duoc, so dong =", n)
spark.stop()
shutil.rmtree(out, ignore_errors=True)
