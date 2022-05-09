package com.daimler.tt.streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//week 16 - session 8
object TumblingWindow extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Tumbling Window")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  val ordersSchema = StructType(List(
    StructField("order_id", IntegerType, true),
    StructField("order_date", StringType, true),
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

  val runningTotal = ordersDF
    .groupBy(window(col("order_date"), "15 minute"))
    .agg(sum("amount").as("totalInvoice"))

  val outputDF = runningTotal.select("window.start", "window.stop", "totalInvoice")

  //Write to Sink
  val ordersQurey = outputDF.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "checkpointDir")
    .trigger(Trigger.ProcessingTime("1 seconds"))
    .start()

  ordersQurey.awaitTermination()

}
