import pyspark.sql
from pyspark.sql.functions import col

spark = pyspark.sql.SparkSession.builder.getOrCreate()

df = spark.table("biobridge_gold.sensors_by_date")
df1 = df.where(col("datum") == "2020-08-04")
df1.show(5)
# print(df1.count())