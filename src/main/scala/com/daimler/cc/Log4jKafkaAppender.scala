package com.daimler.cc
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Log4jKafkaAppender {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Log4j-kafka-appender")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    val sc: SparkContext = ssc.sparkContext

    sc.setLogLevel("ERROR")

    val textStream = ssc.textFileStream("file:///C:/Users/MKONDAP/Desktop/Learning/SparkMavenCC/input")

    textStream.print()

    ssc.start()
    ssc.awaitTermination()


  }
}
