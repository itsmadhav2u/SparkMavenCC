package com.daimler.tt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object LogLevel extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Count log levels")

  val myList = List("WARN: Tuesday 4 September 1722",
                    "ERROR: Tuesday 4 September 1708",
                    "WARN: Tuesday 4 September 1708",
                    "ERROR: Tuesday 4 September 1722",
                    "WARN: Tuesday 4 September 1722",
                    "ERROR: Tuesday 4 September 1708",
                    "ERROR: Tuesday 4 September 1722")

  val baseRDD = sc.parallelize(myList)

  val pairedRDD = baseRDD.map( line => {
    val fields = line.split(":")
    (fields(0),1)
  })

  val outputRDD = pairedRDD.reduceByKey((x,y) => x+y)

  outputRDD.foreach(println)

}
