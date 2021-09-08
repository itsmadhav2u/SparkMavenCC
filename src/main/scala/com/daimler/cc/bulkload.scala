package com.daimler.cc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object bulkload extends App{

  val spark = SparkSession.builder()
    .appName("Bulk load")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load("data.txt")

  df.show(false)

  val cols = df.columns
  val colsortedDF = df.select(cols.map(x=>col(x)):_*)
  val valCols = cols.filterNot(x=>x.equals("id"))

  val rdd =  df.rdd.map(row =>{
    (row(0).toString,(row(1).toString,row(2).toString,row(3)))
  })

  val colfamily = "cf"
  val rdd2 = rdd.flatMap( x => {
    val rowkey = x._1
    for (i <- 0 until valCols.length) yield {
      val colName = valCols(i).toString
      val colValue = x._2.productElement(i)
      (rowkey,(colfamily,colName,colValue))
    }
  })

  rdd2.foreach(println)


  
}
