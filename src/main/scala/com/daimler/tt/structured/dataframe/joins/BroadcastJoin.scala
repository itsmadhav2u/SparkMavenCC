package com.daimler.tt.structured.dataframe.joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BroadcastJoin extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "BroadcastJoin Example")
  sparkConf.set("spark.master", "local[*]")

  val spark =  SparkSession.builder().config(sparkConf).getOrCreate()

  //customer_id - common column
  val customersDF = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/customers.csv")
    .load()

  //order_customer_id - common column
  val ordersDF = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/orders.csv")
    .load()

  val joinCondition = ordersDF.col("order_customer_id") === customersDF.col("customer_id")

  val joinType = "inner"

  //Asking/Forcing spark to broadcast 'customersDF' and do broadcast join (so shuffle required)
  ordersDF.join(broadcast(customersDF), joinCondition , joinType).show()

  spark.stop()
}
