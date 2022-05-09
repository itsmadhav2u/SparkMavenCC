package com.daimler.tt.structured.dataframe.aggregartions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/* Week 12 -  DataFrames Session 17
* 1. Group data based on Country and Invoice Number
* 2. TotalQuantity of each group
* 3. Sum of Invoice value
* In Using Column Object Expression, Using String expression and Using Spark SQL
* */

object GroupingAggregations extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Grouping Aggregations Example")
  sparkConf.set("spark.master", "local[*]")

  val spark =  SparkSession.builder().config(sparkConf).getOrCreate()

  val baseDF: DataFrame = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/order_data.csv")
    .load()

  //Using Column Object Expression
  baseDF.groupBy("Country","InvoiceNo")
    .agg(sum("Quantity").as("TotalQuantity"),
      sum(expr("Quantity * UnitPrice")).as("InvoiceValue")
    ).show(10,false)

  //String expression
  baseDF.groupBy("Country","InvoiceNo")
    .agg(expr("sum(Quantity) as TotalQuantity"),
      expr("sum(Quantity * UnitPrice) as InvoiceValue")
    ).show(10, false)

  //Using Spark SQL
  baseDF.createOrReplaceTempView("invoice")

  spark.sql(
    """select Country, InvoiceNo, sum(Quantity), sum(Quantity * UnitPrice) from invoice
      | group by Country, InvoiceNo""".stripMargin).show(10, false)

  spark.stop()

}
