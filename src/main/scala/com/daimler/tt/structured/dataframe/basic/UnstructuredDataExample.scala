package com.daimler.tt.structured.dataframe.basic

//Week 12 - Session 12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.matching.Regex

object UnstructuredDataExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Handling Unstructured data using RegEx")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val regEx: Regex = """^(\S+) (\S+)\t(\S+),(\S+)"""r

  case class Orders(order_id:Int, customer_id:Int, order_status:String)

  def parser(line:String):Orders ={
    line match {
      case regEx(order_id, date, customer_id, order_status) =>
        Orders(order_id.toInt, customer_id.toInt, order_status)
    }
  }

  val baseRDD = spark.sparkContext.textFile("input/orders_new.txt")

  val parsedRDD: RDD[Orders] = baseRDD.map(parser)

  import spark.implicits._

  val ordersDS: Dataset[Orders] = parsedRDD.toDS.cache()

  ordersDS.printSchema()

  ordersDS.select("order_id").show(false)

  ordersDS.groupBy("order_status").count().show(false)

  scala.io.StdIn.readLine()
  spark.stop()


}
