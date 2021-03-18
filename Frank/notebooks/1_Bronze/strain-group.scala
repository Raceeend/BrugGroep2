// Databricks notebook source
spark
  .read
  .option("delimiter", ";")
  .option("header", true)
  .csv("/mnt/biobridge/raw/strain-group*.csv.gz")
  .withColumnRenamed("Sensor naam", "Sensor_naam")
  .withColumnRenamed("Primaire lijn", "Primaire_lijn")
  .write
  .saveAsTable("biobridge_bronze.strain")