package com.daimler.tt.streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

//Week 16 - Session 5
object FileSource extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("File Source Example")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  //Reading streaming data from Directory as source
  val ordersStreamDF = spark.readStream
    .format("json")
    .option("path","streamInput")
    .option("maxFilesPerTrigger", 17) //to balance load
    .load()

 /* Other useful options to clean up processes data
    .option("cleanSource", "delete")  //other choices are  "archive", "off" (default)
    if we use "archive" for cleanSource then we need to give path where to archive the processed data using below option
    .option("sourceArchiveDir", "/path/to/processed/archive") *
 * */

  //Processing streaming data
  ordersStreamDF.createOrReplaceTempView("orders")
  val completedOrdersDF = spark.sql("select * from orders where order_status='COMPLETE'")

  //Write Streaming data to sink
  val streamQuery = completedOrdersDF.writeStream
    .format("json")
    .outputMode("append")
    .option("path","streamOutput")
    .option("checkpointLocation", "checkPointDir")
    .trigger(Trigger.ProcessingTime("17 seconds"))
    .start()

  println("Streaming Job Started")
  streamQuery.awaitTermination()

}
