// Databricks notebook source
spark
  .read
  .option("delimiter", ";")
  .option("header", true)
  .csv("/mnt/biobridge/raw/opzetstukken.csv.gz")
  .withColumnRenamed("Opzetstuk Noord (°)", "Opzetstuk_Noord")
  .withColumnRenamed("Opzetstuk Zuid (°)", "Opzetstuk_Zuid")
  .write
  .mode("overwrite")
  .saveAsTable("biobridge_bronze.opzetstukken")