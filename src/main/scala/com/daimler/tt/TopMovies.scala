package com.daimler.tt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/*
1. At least 100 people should have rated that movie
2. Movie with rating avg >= 4.0
 */
object TopMovies extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Find Top Movies")

  val baseRatingsRDD = sc.textFile("input/ratings.dat")

  val mappedRatingsRDD = baseRatingsRDD.map(line => (line.split("::")(1), line.split("::")(2)))

  val mappedRatingsRDD2 = mappedRatingsRDD.mapValues(pair => (pair.toFloat, 1.0))

  val totalRatingsRDD = mappedRatingsRDD2.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

  val topRatedMoviesRDD =  totalRatingsRDD.filter(pair => pair._2._2 > 100)

  val avgRatingsRDD = topRatedMoviesRDD.mapValues(pair =>  pair._1 / pair._2)

  val finalRatingRDD = avgRatingsRDD.filter(rating => rating._2 >= 4.0)

  //finalRatingRDD.collect().foreach(println)

  val baseMoviesRDD = sc.textFile("input/movies.dat")

  val moviesMappedRDD = baseMoviesRDD.map(line => (line.split("::")(0), line.split("::")(1)))

  val joinedRDD = moviesMappedRDD.join(finalRatingRDD)

  val outputRDD = joinedRDD.map(pair => pair._2._1)

  outputRDD.take(17).foreach(println)

  scala.io.StdIn.readLine()

}
