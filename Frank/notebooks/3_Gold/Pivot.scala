// Databricks notebook source
import org.apache.spark.sql.functions.first

spark.table("biobridge_silver.strain")
  .groupBy($"datum_tijd")
  .pivot($"Sensor_naam")
  .agg(
    first("Waarde").as("waarde"),
    first("Kopafstand").as("kopafstand"),
    first("Element").as("element")
  )
  .withColumn("datum", to_date($"Datum_tijd"))
  .write
  .mode("overwrite")
  .partitionBy("datum")
  .saveAsTable("biobridge_gold.pivot")

// COMMAND ----------

display(spark.table("biobridge_gold.pivot"))

// COMMAND ----------

import org.apache.spark.sql.functions.avg

spark.table("biobridge_gold.pivot")
  .where($"datum" >= "2020-08-01" and $"datum" < "2020-09-01")
  .groupBy($"datum")
  .agg(
    first("*")
  )
  .orderBy($"datum")
.show(100)

// COMMAND ----------


