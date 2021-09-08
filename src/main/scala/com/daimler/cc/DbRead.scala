package com.daimler.cc
import com.daimler.cc.KafkaToHive.spark
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object DbRead extends App{
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("Spark SQL Table Demo")
    .master("local[3]")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.sql

  val a = sql("select * from Ro45.log_data")

  a.show(false)
}
