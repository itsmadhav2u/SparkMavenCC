package com.daimler.tt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RatingCount extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Count Ratings")

  val baseRDD: RDD[String] = sc.textFile("input/moviedata-201008-180523.data")
  //Data: user_id, movie_id, ratings, epoch_timestamp

  val rdd1: RDD[String] = baseRDD.map(line => line.split("\t")(2))

  println("========== Using map + reduceByKey (Transformations : returns a RDD) =========")

  val ratingsRDD: RDD[(String, Int)] = rdd1.map(rating => (rating, 1))

  val outputRDD: RDD[(String, Int)] = ratingsRDD.reduceByKey((a, b) => a+b)

  outputRDD.collect.foreach(println)

  println("========== Using countByValue =========")
  // Action : returns a local variable (so use only if this is the final operation or else go with transformations
  val outputRDD2: collection.Map[String, Long] = rdd1.countByValue()

  outputRDD2.foreach(println)

}
