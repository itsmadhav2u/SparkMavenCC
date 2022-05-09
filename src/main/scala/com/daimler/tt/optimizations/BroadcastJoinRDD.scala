package com.daimler.tt.optimizations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object BroadcastJoinRDD extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Salting Example")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  //Big table
  val data1 = List(
      "WARN,2016-12-31 04:17:32",
      "ERROR,2016-12-31 03:10:32",
      "WARN,2016-12-31 07:59:32",
      "ERROR,2015-5-21 09:19:32",
      "ERROR,2015-4-11 04:39:32"
    )

  //Small table
  val data2 = Array(("ERROR", 0), ("WARN", 1))

  val baseRDD1 = spark.sparkContext.parallelize(data1)

  val baseRDD2 = spark.sparkContext.parallelize(data2)

  val mappedRDD = baseRDD1.map(line => (line.split(",")(0), line.split(",")(1)))

  //Normal Join
  val outputRDD = mappedRDD.join(baseRDD2)

  outputRDD.collect.foreach(println)

  println("============================================================")

  //Broadcast Join
  val keymap = data2.toMap

  val broadcastedMap: Broadcast[Map[String, Int]] = spark.sparkContext.broadcast(keymap)

  val newOutputRDD = mappedRDD.map( pair => (pair._1, (pair._2, broadcastedMap.value(pair._1))))

  newOutputRDD.collect.foreach(println)

  scala.io.StdIn.readLine()

  spark.stop()

}
