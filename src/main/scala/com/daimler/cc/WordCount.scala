package com.daimler.cc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Log4j-kafka-appender")
  val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
  val sc: SparkContext = ssc.sparkContext

  val textFile = sc.textFile("file:///C:/Users/MKONDAP/Desktop/Learning/SparkMavenCC/input")
  val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  counts.foreach(println)

}
