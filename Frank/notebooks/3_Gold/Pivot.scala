// Databricks notebook source
import org.apache.spark.sql.functions._

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
