package com.daimler.cc

import org.apache.spark.rdd
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object test extends App{

  val spark = SparkSession.builder()
    .appName("Spark Kafka To Hive")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  val schema = StructType(
    StructField("firstName", StringType, true) ::
      StructField("lastName", IntegerType, false) ::
      StructField("middleName", IntegerType, false) :: Nil)

  import spark.implicits._
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))


  val rdd = spark.sparkContext.parallelize(data)
  val dfFromRDD1 = rdd.toDF("language","users_count")
  val emptydf = spark.createDataFrame(spark.sparkContext
    .emptyRDD[Row], schema)
  dfFromRDD1.show(false)
  emptydf.show(false)
  val df = spark.emptyDataFrame
  println(!dfFromRDD1.head(1).isEmpty)
  println(!emptydf.head(1).isEmpty)

}
