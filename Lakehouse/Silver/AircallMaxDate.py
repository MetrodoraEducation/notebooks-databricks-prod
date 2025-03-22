# Databricks notebook source
maxdate_df = spark.sql("select cast(cast(max(started_at) as date) as string) as maxdate from silver_lakehouse.aircallcalls").collect()[0][0]
dbutils.notebook.exit(maxdate_df)
