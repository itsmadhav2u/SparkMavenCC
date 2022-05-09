package com.daimler.tt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object WordCount extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  //val spark =  SparkSession.builder().master("local[*]").appName("Word Count").getOrCreate()
  val sc = new SparkContext("local[*]", "Word Count")

  val baseRDD = sc.textFile("input/input.txt")

  val wordsRDD = baseRDD.flatMap(line => line.split(" "))

  val keyValueRDD = wordsRDD.map(word => (word.toLowerCase, 1))

  val wordCountRDD = keyValueRDD.reduceByKey((a,b) => a+b)

  //Word Count Output
  wordCountRDD.take(10).foreach(println)

  println("=============== Sort by count using sortBy method ============================")
  val sortedRDD = wordCountRDD.sortBy(pair => pair._2,false)

  sortedRDD.take(10).foreach(println)

  println("================= Sort by count using sortByKey method ==========================")
  //sortByKey sorts by key only so we need to reverse the RDD to make count as key
  val reversedRDD = wordCountRDD.map(pair => (pair._2, pair._1))

  //By default sortByKey is true sorts in ascending order so we need to set to false to make it descending order
  val sortByCount = reversedRDD.sortByKey(false).map(pair => (pair._2, pair._1))

  sortByCount.take(10).foreach(println)



  //scala.io.StdIn.readLine()

}
