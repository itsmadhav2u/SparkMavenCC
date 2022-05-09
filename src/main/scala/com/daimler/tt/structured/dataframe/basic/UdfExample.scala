package com.daimler.tt.structured.dataframe.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, expr, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Person(name:String, age:Int, city:String)

object UdfExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","UDF example")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  def ageChecker(age:Int):String ={
    if(age > 25) "Y" else "N"
  }

  val rawDF = spark.read.format("csv")
    .option("inferSchema", true)
    .option("path", "input/dataset1")
    .load

  val namedDF: DataFrame = rawDF.toDF("name","age","city")

  import spark.implicits._
  val DS: Dataset[Person] = namedDF.as[Person]

  //Column Object Expression UDF
  val ageCheckUDF1: UserDefinedFunction = udf(ageChecker(_:Int):String)
  val output1DF = namedDF.withColumn("adult", ageCheckUDF1(col("age")))
  output1DF.show(false)

  //SQL/String expression UDF
  spark.udf.register("ageCheckUDF2",ageChecker(_:Int):String)
  val output2DF = namedDF.withColumn("adult", expr("ageCheckUDF2(age)"))
  output2DF.show(false)

  //Using anonymous function in SQL/String expression UDF
  spark.udf.register("ageCheckUDF3", (age:Int) => {if (age >25) "Y" else "N"})
  val output3DF = namedDF.withColumn("adult", expr("ageCheckUDF3(age)"))
  output3DF.show(false)

 /*
 Note : when we use SQL/String expression UDF, UDF will be registered in spark catalog.
  But when we use Column Object Expression UDF way it will not be registered.
  */

  spark.catalog.listFunctions().filter( x => x.name.contains("ageCheckUDF")).show()

  //If it is registered in catalog we can use it for normal spark sql commands as well
  namedDF.createOrReplaceTempView("peopleTable")

  spark.sql("select name, age, city, ageCheckUDF2(age) from peopleTable").show()

  spark.stop()

}
