package com.daimler.tt.structured.dataframe.aggregartions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.sum
/*  Week 12 -  DataFrames Session 18
* 1. Partition column - country
* 2. Ordering column - weeknum
* 3. Window Size - from 1st row of the group to current row
* */
object WindowAggregations extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Window Aggregations Example")
  sparkConf.set("spark.master", "local[*]")

  val spark =  SparkSession.builder().config(sparkConf).getOrCreate()

  val baseDF: DataFrame = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/windowdata.csv")
    .load()

  /*
  unboundedPreceding - starting row
  -1 - previous row
  -n - previous N rows
  */
  val myWindow: WindowSpec = Window.partitionBy("country")
                        .orderBy("weeknum")
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  baseDF.withColumn("RunningTotal", sum("invoicevalue").over(myWindow)).show(false)

  spark.stop

}
