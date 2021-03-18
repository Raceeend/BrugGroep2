// Databricks notebook source
spark
  .read
  .option("delimiter", ";")
  .option("header", true)
  .csv("/mnt/biobridge/raw/meteo.csv.gz")
  .write
  .mode("overwrite")
  .saveAsTable("biobridge_bronze.meteo")