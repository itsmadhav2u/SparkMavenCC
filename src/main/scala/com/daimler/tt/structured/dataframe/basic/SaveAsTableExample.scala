package com.daimler.tt.structured.dataframe.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object SaveAsTableExample extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SaveAsTable Example")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()


  val ordersDF= spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/orders.csv")
    .load


  //By default it will be saved inside default database

  spark.sql("create database if not exists tt")

  ordersDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("ro45.orders")

  spark.catalog.listTables("ro45").show()

  spark.stop()
}
