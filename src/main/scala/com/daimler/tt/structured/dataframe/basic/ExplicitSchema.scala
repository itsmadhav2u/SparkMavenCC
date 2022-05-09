package com.daimler.tt.structured.dataframe.basic

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class Orders(order_id:Int, order_date:Timestamp, order_customer_id:Int, order_status:String)

object ExplicitSchema extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","Explicit schema Example")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  //EXPLICIT Schema Using programmatic way

  println("================= EXPLICIT Schema Using programmatic way ====================")

  val orderSchmea = StructType(List(
    StructField("orderId", IntegerType, true),
    StructField("orderDate", TimestampType, true),
    StructField("customerId", IntegerType, true),
    StructField("orderStatus", StringType, true)
  ))

  val ordersDF:Dataset[Row] = spark.read.format("csv")
    .schema(orderSchmea)
    .option("header",true)
    .option("path", "input/orders.csv")
    .load

  ordersDF.printSchema()

  ordersDF.show(5)

  //EXPLICIT Schema Using DDL string

  println("================= EXPLICIT Schema Using DDL string ====================")

  val orderSchmeaDDL = "order_Id Int, order_Date Timestamp, customer_Id Int, order_Status String"

  val ordersDF2:Dataset[Row] = spark.read.format("csv")
    .schema(orderSchmeaDDL)
    .option("header",true)
    .option("path", "input/orders.csv")
    .load

  ordersDF2.printSchema()

  ordersDF2.show(5)

  //Dataset
  println("================= DataSet Using Case Class====================")

  import spark.implicits._

  val ordersDF3:Dataset[Row] = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/orders.csv")
    .load

  val ordersDS: Dataset[Orders] = ordersDF3.as[Orders]

  ordersDS.printSchema()

  ordersDS.show(5)

  spark.stop()
}
