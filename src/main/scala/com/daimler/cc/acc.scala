package com.daimler.cc

import org.apache.spark.sql.SparkSession

object acc extends App{
  val spark = SparkSession.builder()
    .appName("Spark Kafka To Hive")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

val acc= spark.sparkContext.collectionAccumulator[Char]
  val rdd=spark.sparkContext.parallelize(Seq('1','2','3','4','5'))
  var df= Seq('1','2','3')

}
