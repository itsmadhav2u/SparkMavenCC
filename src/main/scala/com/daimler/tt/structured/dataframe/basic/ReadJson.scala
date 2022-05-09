package com.daimler.tt.structured.dataframe.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadJson extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Load Json and Handle incorrect data")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  /* For Json Spark will auto infer the schema
    Read Modes:
    1. PERPERMISSIVE
    {
    Default Mode
    sets other fields to null when it meets a corrupted record,
    and puts the malformed string into a new field configured by columnName Of '_corrupt_record'
    }
    2. DROPMALFORMED
    3. FAILFAST
   */
  val playersDF = spark.read.format("json")
    .option("mode", "DROPMALFORMED")
    .load("input/players.json")

  playersDF.show(false)

  spark.stop()


}
