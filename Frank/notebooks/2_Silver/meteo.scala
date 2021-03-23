// Databricks notebook source
import org.apache.spark.sql.functions.{to_date, to_timestamp, translate}

spark.table("biobridge_bronze.meteo")
  .where($"Datum-tijd".isNotNull) // Filter extra header
  .select(
    to_timestamp($"Datum-tijd").as("Datum-tijd"),
    translate($"Temp", ",", ".").cast("Double").as("Temp"),
    translate($"Windsnelheid", ",", ".").cast("Double").as("Windsnelheid"),
    translate($"Windrichting", ",", ".").cast("Double").as("Windrichting"),
    translate($"Luchtvochtigheid", ",", ".").cast("Double").as("Luchtvochtigheid"),
    translate($"Luchtdruk", ",", ".").cast("Double").as("Luchtdruk"),
    translate($"Neerslag", ",", ".").cast("Double").as("Neerslag"),
    translate($"Zonneschijn", ",", ".").cast("Double").as("Zonneschijn"),
    to_date($"Datum-tijd").as("datum")
  )
  .write
  .mode("overwrite")
  .partitionBy("datum")
  .saveAsTable("biobridge_silver.meteo")
