package com.daimler.tt.structured.dataframe.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object DataFrameWriterExample extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appp.name", "DataFrame Write Example")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val ordersDF:Dataset[Row] = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","input/orders.csv")
    .load

  /*
  1. By default spark writes in Parquet Format
  2. SaveModes
    1. Append
    2. Overwrite
    3. ErrorIfExists
    4. Ignore
  3. Output path should be a folder
   */

  println("Number of partitions is : " + ordersDF.rdd.getNumPartitions)

  //When we are writing number of files in output folder  = Number of partitions
  ordersDF.write.format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "output/orders")
    .save()

  /*
  ================= Writing data using PartitionBy ====================================

  Using PartitionBy we can group the data on a column before writing on a particular column/columns
  One folder will be created for each unique value of partition column (Similar to Hive) inside output directory
  This will help to filter lot of data when we are reading/looking for some particular data (Partition Pruning)
  Note:
  -> We can partition the data by multiple columns as well
  ->Make Sure cardinality of our partition column is low
   */
  ordersDF.write.format("csv")
    .mode(SaveMode.Overwrite)
    .partitionBy("order_status")
    .option("path", "output/orders_partitioned")
    .save()

  /*
  ================= Writing data using BucketBy ====================================
  Using Hash FUnction it will divide the data into Number of buckets asked based on the coulmn/columns given
  Important Note: bucketBy doesn't work with save(), works only with saveAsTable
   */

  /*ordersDF.write
    .mode(SaveMode.Overwrite)
    .bucketBy(4, "order_id")
    .saveAsTable("orders_bucketed")*/

  /*
  ================= Writing data using maxRecordsPerFile ====================================
  We can limit number of records in file using maxRecordsPerFile
   */

  ordersDF.write.format("csv")
    .mode(SaveMode.Overwrite)
    .option("maxRecordsPerFile", 5000)
    .option("path", "output/orders_2000_perfile")
    .save()

spark.stop()
}
