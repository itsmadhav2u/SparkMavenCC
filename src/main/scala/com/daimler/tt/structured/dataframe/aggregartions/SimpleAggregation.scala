package com.daimler.tt.structured.dataframe.aggregartions

/* Week 12 -  DataFrames Session 16
1. Find TotalNumberOfRows, TotalQuantity, AvgUnitPrice, NumberOfUniqueInvoices
  Using Column Object Expression
  Using String expression
  Using Spark SQL
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SimpleAggregation extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Simple Aggregations Example")
  sparkConf.set("spark.master", "local[*]")

  val spark =  SparkSession.builder().config(sparkConf).getOrCreate()

  val baseDF: DataFrame = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/order_data.csv")
    .load()

  //Using Column Object Expression
  baseDF.select(count("*").as("RowCount"),
    sum("Quantity").as("TotalQuantity"),
    avg("UnitPrice").as("AvgPrice"),
    countDistinct("InvoiceNo").as("CountDistinct")
  ).show()

  //Using String expression
  baseDF.selectExpr("count(*) as RowCount",
  "sum(Quantity) as TotalQuantity",
    "avg(UnitPrice) as AvgPrice",
    "count(Distinct(InvoiceNo)) as CountDistinct"
  ).show()

  //Using Spark SQL
  baseDF.createOrReplaceTempView("invoice")

  spark.sql("select count(*), sum(Quantity), avg(UnitPrice), count(Distinct(InvoiceNo)) from invoice").show()

  spark.stop()
}
