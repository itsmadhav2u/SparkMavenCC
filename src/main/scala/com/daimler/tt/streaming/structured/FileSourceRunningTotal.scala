package com.daimler.tt.streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
//Week 16 - Session 6
object FileSourceRunningTotal extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Running Total")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val ordersDF = spark.readStream
    .format("json")
    .option("path","streamInput")
    .option("maxFilesPerTrigger", 2)
    .load()

  ordersDF.createOrReplaceTempView("orders")
  val completedOrdersCount = spark.sql("select count(*) from orders where order_status='COMPLETE'")

  val runningTotalQuery = completedOrdersCount.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "checkpointDir")
    .trigger(Trigger.ProcessingTime("17 seconds"))
    .start()

  runningTotalQuery.awaitTermination()




}
