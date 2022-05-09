package com.daimler.tt.structured.dataframe.usecases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType
/*
Use case requirements:
1. Create a Scala List and convert in to DataFrame (order_id, order_date, customer_id, order_status)
2. Convert date to epoc timestamp (number of seconds after 1st jan 1970)
3. Create new column with "newId" and make it unique
4. Drop duplicates (order_date, customer_id)
5. Drop column
6. Sort By order_date
*/
object Case1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Use case 1")
  sparkConf.set("spark.master", "local[*]")

  val spark =  SparkSession.builder().config(sparkConf).getOrCreate()

  val list = List((1, "2021-02-17", 1722, "CLOSED"),
    (2, "2021-02-18", 822, "PENDING_PAYMENT"),
    (3, "2021-02-17", 1722, "CLOSED"),
    (4, "2021-02-22", 1217, "COMPLETE"))

  //1
  val baseDF = spark.createDataFrame(list).toDF("order_id", "order_date", "customer_id", "order_status")

  //2 (using withColumn we can add new or change existing column
  val newDF1 = baseDF.withColumn("order_date", unix_timestamp(col("order_date").cast(DateType)))

  //3 (Adding a column with unique values)
  val newDF2 = newDF1.withColumn("newId", monotonically_increasing_id())

  //4
  val newDF3 = newDF2.dropDuplicates("order_date","customer_id")

  //5.
  val newDF4 = newDF3.drop("order_id")

  //6.
  val outputDF =  newDF4.sort("order_date")

  outputDF.printSchema()

  outputDF.show()

  spark.stop()
}
