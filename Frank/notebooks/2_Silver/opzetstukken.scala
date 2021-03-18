// Databricks notebook source
import org.apache.spark.sql.functions.{to_timestamp, translate}

spark.table("biobridge_bronze.opzetstukken")
  .select(
    to_timestamp($"Datum-tijd").as("Datum-tijd"),
    translate($"Opzetstuk_Noord", ",", ".").cast("Double").as("Opzetstuk_Noord"),
    translate($"Opzetstuk_Zuid", ",", ".").cast("Double").as("Opzetstuk_Zuid")
  )
  .write
  .mode("overwrite")
  .saveAsTable("biobridge_silver.opzetstukken")
