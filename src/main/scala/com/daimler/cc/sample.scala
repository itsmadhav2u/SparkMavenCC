package com.daimler.cc
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Application").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc: SparkContext = ssc.sparkContext
    sc.setLogLevel("ERROR")
    val input = ssc.textFileStream("file:///C:/Users/MKONDAP/Desktop/Learning/SparkMavenCC/input")
    val lines = input.flatMap(_.split(" "))
    val words = lines.map(word => (word, 1))
    val counts = words.reduceByKey(_ + _)
    counts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
