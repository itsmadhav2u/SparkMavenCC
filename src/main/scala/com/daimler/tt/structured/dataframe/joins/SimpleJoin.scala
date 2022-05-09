package com.daimler.tt.structured.dataframe.joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

//Week 12 -  DataFrames Session 19,20
//Simple Join or Shuffle Sort Merge Join
object SimpleJoin extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Simple Join (Shuffle Sort Merge Join) Example")
  sparkConf.set("spark.master", "local[*]")

  val spark =  SparkSession.builder().config(sparkConf).getOrCreate()

  //customer_id - common column
  val customersDF = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/customers.csv")
    .load()

  //order_customer_id - common column
  val ordersDF = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/orders.csv")
    .load()

  val joinCondition = ordersDF.col("order_customer_id") === customersDF.col("customer_id")

  /*
  Join Type available
  inner - matching records from both tables
  outer - matching records + non matching records from left table and right table
  left - matching records + non matching records from left table
  right - matching records + non matching records from right table
   */
  val joinType = "inner"

  /*
  Set below property to Avoid BroadcastJoin even if it is possible so only Shuffle Sort Merge Join will happen
  spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
  */

  //leftTable.join(rightTable, joinCondition, joinType)
  val joinedDF = ordersDF.join(customersDF, joinCondition , joinType).sort("order_id")

  /*
   Note: If we have same column name in both dataframes/tables it will lead to ambiguous problem when we select the column
          (as there will be 2 cols with same name)
   To solve this we have to ways
   1. Before Join : Rename the column in one of the table using df.withColumnRenamed(oldName, newName)
   2. After Join : Drop column from one of the table with .drop(df.col("column_name"))
  * */

  //========== Replacing or Handling 'null' values using coalesce in .withColumn=========

  /* In below example we are using coalesce on 'order_id' column, I
    - if order_id has some value is present then original value will be printed
    - if order_id value is null then -1 (in this case) will be printed
    */

  val outputDF = joinedDF.withColumn("order_id", expr("coalesce(order_id, -1)"))

  outputDF.show()

  spark.stop()
}
