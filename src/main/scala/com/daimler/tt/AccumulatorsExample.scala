package com.daimler.tt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AccumulatorsExample extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Count Empty Lines")

  val baseRDD = sc.textFile("input/sampleFile.txt")

  //myAccum is programmaticall name and 'Blank Lines Accumulators' is the name for the webUI
  val myAccum = sc.longAccumulator("Blank Lines Accumulators")
  //It will be initialised with value 0

  baseRDD.foreach(line => if(line == "") myAccum.add(1))

  println(myAccum.value)

}
