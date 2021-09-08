package com.daimler.cc
import org.json4s.jackson.JsonMethods._
import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object json extends App{

  val spark: SparkSession = SparkSession.builder().master("local[3]").appName("Streaming Word Count").getOrCreate()

  val jsonString =
    """
      |{"timestamp": "2021-04-07 15:42:37", "data": "AAPL,ro45 1 2001,10.81", "error": "java.text.ParseException: Unparseable date: 'ro45 1 2001'"}
      |""".stripMargin
  val jsonMap = parse(jsonString).values.asInstanceOf[Map[String, String]]
  println(jsonMap)
}
