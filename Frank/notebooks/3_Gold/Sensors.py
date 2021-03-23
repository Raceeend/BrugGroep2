# Databricks notebook source
# MAGIC %pip install git+https://github.com/databrickslabs/tempo.git

# COMMAND ----------

from tempo import TSDF

meteo_tsdf = TSDF(spark.table("biobridge_silver.meteo"), ts_col="Datum-tijd", partition_cols = ["datum"])
strain_tsdf = TSDF(spark.table("biobridge_silver.strain"), ts_col="Datum-tijd", partition_cols = ["datum"])

# COMMAND ----------

joined_df = strain_tsdf.asofJoin(meteo_tsdf, right_prefix="meteo").df

# COMMAND ----------

joined_df \
  .write \
  .mode("overwrite") \
  .saveAsTable("biobridge_gold.sensors")
