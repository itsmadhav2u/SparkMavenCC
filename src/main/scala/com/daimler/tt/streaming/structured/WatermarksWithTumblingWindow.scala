package com.daimler.tt.streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

//week 16 - session 9
object WatermarksWithTumblingWindow extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Watermarks with Tumbling Window")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  val ordersSchema = StructType(List(
    StructField("order_id", IntegerType, true),
    StructField("order_date", TimestampType, true),
    StructField("order_customer_id", IntegerType, true),
    StructField("order_status", StringType, true),
    StructField("amount", IntegerType, true)
  ))

  //Reading a stream data from a source
  val linesDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 1722)
    .load()

  //Process
  val tempOrdersDF = linesDF.select(from_json(col("value"), ordersSchema).as("value"))

  val ordersDF = tempOrdersDF.select("value.*")

  /*
  Watermarks are like expiry date for records based on event time
  It helps to cleanup statestore by removing data before watermark durration
  Note : .withWatermark() should be before .groupBy()'
  */
  val runningTotal = ordersDF
    .withWatermark(eventTime = "order_date",  delayThreshold = "30 minute")
    .groupBy(window(col("order_date"), windowDuration = "15 minute"))
    .agg(sum("amount").as("totalInvoice"))

  val outputDF = runningTotal.select("window.start", "window.stop", "totalInvoice")

  //Write to Sink
  /*
  ** Note: 1. When we use watermark we should not use complete mode as complete mode needs all records to print
  but watermark need to cleanup state store, in this case watermark ignored and it cannot clean state store (side effects)
  So we need to use either watermarks or complete output mode
  2. Append mode is not allowed on Window Aggregates as it think there won't be any update happen
     But Spark allow's append mode for watermarks on event time
     append mode will suppress output of window aggregates unless they cross water mark boundary
  3. Update mode is most useful and efficient
  */
  val ordersQurey = outputDF.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "checkpointDir")
    .trigger(Trigger.ProcessingTime("1 seconds"))
    .start()

  ordersQurey.awaitTermination()

}
