package com.daimler.cc

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

import org.apache.spark.sql.functions.{col, udf, unix_timestamp}
object test2 extends App{
  val spark = SparkSession.builder()
    .appName("Spark Kafka To Hive")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.sql
  import spark.implicits._
  val tsConversionToLongUdf = udf((ts: java.sql.Timestamp) => ts.getTime)
  val df = Seq(
    (1, Timestamp.valueOf("2021-08-27 08:01:13.462")),
    (1, Timestamp.valueOf("2021-08-27 08:04:10.976"))
  ).toDF("typeId","eventTime")

  df.show(false)
  df.printSchema()
  val df2=df.withColumn("epoc",col("eventTime").cast("long"))
    .withColumn("epoc2",unix_timestamp(col("eventTime"),"yyyy-MM-dd HH:mm:ss.SSS"))
    .withColumn("epoc3",unix_timestamp(col("eventTime")))
    .withColumn("udf_epoc", tsConversionToLongUdf(col("eventTime")))
  df2.show(false)
  df2.printSchema()


}
