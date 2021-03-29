// Databricks notebook source
import org.apache.spark.sql.functions.{to_date, to_timestamp, translate}

spark.table("biobridge_bronze.opzetstukken")
  .select(
    to_timestamp($"Datum-tijd").as("Datum_tijd"),
    translate($"Opzetstuk_Noord", ",", ".").cast("Double").as("Opzetstuk_Noord"),
    translate($"Opzetstuk_Zuid", ",", ".").cast("Double").as("Opzetstuk_Zuid"),
    to_date($"Datum-tijd").as("datum")
  )
  .where($"datum" >= "2020-05-01")
  .write
  .mode("overwrite")
  .saveAsTable("biobridge_silver.opzetstukken")
