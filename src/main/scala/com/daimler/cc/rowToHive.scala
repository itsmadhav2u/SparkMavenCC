package com.daimler.cc

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.jackson.JsonMethods.mapper

case class mad(name:String, row:Row)

object rowToHive extends App{
  val spark = SparkSession.builder()
    .appName("Spark Kafka To Hive")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.sql
  import spark.implicits._
  /*val b = Seq(mad("Bijay", Row("bijay")),mad("Arpitha",Row("arpitha")),mad("Viswa",Row("Viswa")))
  val Employee_DataFrame = b.toDF
  Employee_DataFrame.show(false )
  val a = Seq(("Bijay", Row("bijay")),("Arpitha",Row("arpitha")),("Viswa",Row("Viswa"))).toDF("name","row")

  a.show(false)
  val rdd=spark.sparkContext.parallelize(Seq(("Bijay", Row("bijay")),("Arpitha",Row("arpitha")),("Viswa",Row("Viswa"))))
  val df= rdd.toDF("name","row")
  df.show(false)*/
  val a="arpitha"
  val b = "bijay"
  val v= "viswa"
  val rdd=spark.sparkContext.parallelize(Seq(("Bijay",b),("Arpitha",a),("Viswa",v)))
 // val rdd2 = rdd.map(row => (row._1, Row.fromSeq(row._2)))
  rdd.foreach(println)
  def getByteArray(a:String): Array[Byte] = mapper.writeValueAsBytes(a)
  //def getRow(a:Array[Byte]) = new
  val rdd3 =  rdd.map(row => (row._1,getByteArray(row._2)))
  rdd3.foreach(println)

  /*val rdd4 = rdd3.map(row => (row._1,getRow(row._2)))
  rdd4.foreach(println)*/


}
