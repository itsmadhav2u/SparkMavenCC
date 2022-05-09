package com.daimler.tt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Source

object KeywordAmount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Total Spend of Each word")

  var wordSet = sc.broadcast(loadBoringWords)

  val baseRDD = sc.textFile("input/bigdatacampaigndata-201014-183159.csv")

  val mappedRDD = baseRDD.map(line => (line.split(",")(10).toFloat, line.split(",")(0)))

  val wordsRDD = mappedRDD.flatMapValues( value => value.split(" "))

  val finalMappedRDD = wordsRDD.map(pair => (pair._2.toLowerCase, pair._1))

  //Removing any boring or unnecessary words
  val filteredRDD = finalMappedRDD.filter(pair => !wordSet.value(pair._1))

  val totalRDD =  filteredRDD.reduceByKey((a,b) => a+b)

  val outputRDD = totalRDD.sortBy( words => words._2, false)

  outputRDD.take(17).foreach(println)


  def loadBoringWords():Set[String] ={
    var boringWords: Set[String] = Set()
    val words = Source.fromFile("input/boringwords.txt").getLines()
    for( word <- words){
      boringWords += word
    }
    boringWords
  }


}
