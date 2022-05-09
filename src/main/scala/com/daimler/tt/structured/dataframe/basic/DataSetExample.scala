package com.daimler.tt.structured.dataframe.basic

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class Order(order_id:Int, order_date:Timestamp, order_customer_id:Int, order_status:String)

object DataSetExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","Data Set Example")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  val ordersDF:Dataset[Row] = spark.read.format("csv")
    .option("header", true)
    .option("path","input/orders.csv")
    .load

  val ordersDS: Dataset[Order] = ordersDF.as[Order]

  //Compile Safe way (types are bound at compile time
  ordersDS.filter(x => x.order_id < 10)

  //Non compile safe way
  //ordersDS.filter("order_ids < 10")

  ordersDS.show()

  spark.stop()
}
