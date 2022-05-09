package com.daimler.tt.structured.dataframe.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkSQLExample extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appp.name", "Spark SQL Example")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val ordersDF:Dataset[Row] = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/orders.csv")
    .load

  ordersDF.createOrReplaceTempView("orders")

  val countStatus = spark.sql("select order_status, count(*) as status_count from orders group by order_status order by status_count desc")

  countStatus.show(false)

  spark.sql("select order_customer_id, count(*) as orders_count from orders where order_status = 'COMPLETE' group by order_customer_id order by orders_count desc").show()

  spark.stop()

}
