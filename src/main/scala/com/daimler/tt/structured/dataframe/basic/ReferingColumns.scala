package com.daimler.tt.structured.dataframe.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, concat, expr}

object ReferingColumns extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Ways to refer column names")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val ordersDF = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/orders.csv")
    .load

  // Refer columns using column name string
  ordersDF.select("order_id", "order_status").show(false)

  // Refer columns using col object
  import spark.implicits._
  ordersDF.select(column("order_id"), col("order_date"), $"order_customer_id", 'order_status).show(false)

  //We cannot mix col string and col object in a single select as below, It will give error
  //ordersDF.select("order_id", col("order_status"))

  //================ Column Expression ==================
  //We cannot mix column string with expr(), we have to use column object in normal select
  //expr() is used to convert column expression into column object so we can select other columns using col objects
  ordersDF.select(col("order_id"), expr("concat(order_status, '_STATUS')")).show(false)

  //TO use column string then we need to use selectExpr instead of normal select
  ordersDF.selectExpr("order_id", "order_date", "concat(order_status, '_STATUS')").show(false)

  spark.stop()

}
