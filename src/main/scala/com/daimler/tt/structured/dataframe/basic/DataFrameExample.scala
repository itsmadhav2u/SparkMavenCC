package com.daimler.tt.structured.dataframe.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DataFrameExample extends App {

  //val spark = SparkSession.builder().appName("Sample").master("local[*]").getOrCreate()
  //or

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appp.name", "Sample")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val ordersDF: Dataset[Row] = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("input/orders.csv")

  ordersDF.printSchema()

  val transformedDF: Dataset[Row] = ordersDF
    .repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id", "order_customer_id")
    .groupBy("order_customer_id")
    .count()


  transformedDF.foreach(x => {
    println(x)
  })

  //transformedDF.show(false)

  //To add our own log messages
  Logger.getLogger(getClass.getName).info("My Application successful")

  scala.io.StdIn.readLine()
  spark.stop()
}
