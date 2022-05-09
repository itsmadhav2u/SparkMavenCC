package com.daimler.tt.structured.dataframe.usecases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SaltingExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "Salting Example")
    sparkConf.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val random = new scala.util.Random
    val start = 1
    val end = 17

    /*
     Sample Data:
        "WARN,2016-12-31 04:17:32",
        "ERROR,2016-12-31 03:10:32",
        "WARN,2016-12-31 07:59:32",
        "ERROR,2015-5-21 09:19:32",
        "ERROR,2015-4-11 04:39:32"
    */

    val baseRDD = spark.sparkContext.textFile("input/saltingExampleData")

    val saltedRDD = baseRDD.map( line => {
      var num = start + random.nextInt((end - start) + 1)
      (line.split(":")(0) + num, line.split(":")(1))
    })

    val groupedRDD = saltedRDD.groupByKey

    val saltedOutputRDD = groupedRDD.map(pair => (pair._1, pair._2.size))

    val saltRemovedRDD = saltedOutputRDD.map( pair =>{
      if(pair._1.substring(0,4) == "WARN")
        ("WARN", pair._2)
      else
        ("ERROR", pair._2)
    })

    val finalOutput = saltedOutputRDD.reduceByKey( _ + _)

    spark.stop()

  }

}
