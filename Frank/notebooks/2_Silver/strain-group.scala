// Databricks notebook source
import org.apache.spark.sql.functions.{to_date, to_timestamp, translate}

spark.table("biobridge_bronze.strain")
  .select(
    to_timestamp($"Datum-tijd").as("Datum_tijd"),
    translate($"Sensor_naam", " ", "").as("Sensor_naam"),
    translate($"Waarde", ",", ".").cast("Double").as("Waarde"),
    $"Unit",
    $"Brugdeel",
    translate($"Kopafstand", ",", ".").cast("Double").as("Kopafstand"),
    $"Element",
    $"Primaire_lijn",
    to_date($"Datum-tijd").as("datum")
  )
  .where($"datum" >= "2020-05-01")
  .write
  .mode("overwrite")
  .partitionBy("datum")
  .saveAsTable("biobridge_silver.strain")
