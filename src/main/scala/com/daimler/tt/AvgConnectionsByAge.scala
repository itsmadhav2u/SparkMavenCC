package com.daimler.tt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AvgConnectionsByAge extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line:String) ={
    val fields = line.split("::")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  val sc = new SparkContext("local[*]"," Avg connections by age")

  val baseRDD =  sc.textFile("input/friendsdata-201008-180523.csv")
  //Data: id, name, age, NumFriends

  val rdd1 = baseRDD.map(parseLine)

  //val rdd2 =  rdd1.map(pair => (pair._1,(pair._2, 1)))
  //Note: Not much difference between map or mapValues
  val rdd2 =  rdd1.mapValues(value => (value, 1))

  val rdd3 = rdd2.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

  //val outputRDD = rdd3.map(pair => (pair._1, pair._2._1/pair._2._2)).sortBy(pair => pair._2,false)
  val outputRDD = rdd3.mapValues(pair => pair._1/pair._2).sortBy(pair => pair._2,false)

  outputRDD.collect.foreach(println)

}
