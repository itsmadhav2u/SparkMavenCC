package com.daimler.cc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.json4s.jackson.JsonMethods.mapper

object rddToHive extends App{
  val spark = SparkSession.builder()
    .appName("Spark Kafka To Hive")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.sql
  import spark.implicits._
  //val rdd=spark.sparkContext.parallelize(Seq(("Bijay", "20000",Array("a","b","c")),
   // ("viswa", "100000",Array("a","b","c")), ("arpitha", "3000",Array("c","a","b"))))

  def getByteArray(str:String): Array[Byte] = mapper.writeValueAsBytes(str)
  val rdd=spark.sparkContext.parallelize(Seq(("Bijay", "20000",getByteArray("rohit")),
    ("viswa", "100000",getByteArray("abd")), ("arpitha", "3000",getByteArray("dhoni"))))
  rdd.foreach(println)

  sql("use ro45")
  sql("drop table if exists ro45.nicos")
  sql("drop table if exists ro45.nicos1")
  sql("CREATE TABLE IF NOT EXISTS ro45.nicos (`name` STRING, `amount` STRING, `id` BINARY)")
  sql("CREATE TABLE IF NOT EXISTS ro45.nicos1 (`name` STRING, `amount` STRING, `id` BINARY)")
  rdd.foreach(tuple => tuple._3.foreach(num =>{
    spark.sql(s"insert into ro45.nicos values('${tuple._1}','${tuple._2}','${num}')")
  }
  ))
  rdd.foreach(tuple =>
    spark.sql(s"insert into ro45.nicos1 values('${tuple._1}','${tuple._2}','${tuple._3}')")
  )
  sql("select * from  ro45.nicos").show()
  sql("select * from  ro45.nicos1").show(false)

}
