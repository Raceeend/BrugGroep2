// Databricks notebook source
import org.apache.spark.sql.functions.{to_timestamp, translate}

spark.table("biobridge_bronze.strain")
  .select(
    to_timestamp($"Datum-tijd").as("Datum-tijd"),
    $"Sensor_naam",
    translate($"Waarde", ",", ".").cast("Double").as("Waarde"),
    $"Unit",
    $"Brugdeel",
    translate($"Kopafstand", ",", ".").cast("Double").as("Kopafstand"),
    $"Element",
    $"Primaire_lijn"
  )
  .write
  .mode("overwrite")
  .saveAsTable("biobridge_silver.strain")
