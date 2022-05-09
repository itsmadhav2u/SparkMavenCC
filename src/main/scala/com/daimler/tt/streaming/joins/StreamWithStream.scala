package com.daimler.tt.streaming.joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
//week 16 - session 14
object StreamWithStream extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Stream and Stream DF Left/right/full outer Join")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  /*
  {"impressionID": "100001", "ImpressionTime": "2020-11-01 10:00:00", "CampaignName": "AG India"}
  {"impressionID": "100002", "ImpressionTime": "2020-11-01 10:00:00", "CampaignName": "AG India"}
  {"impressionID": "100003", "ImpressionTime": "2020-11-01 10:40:00", "CampaignName": "AG India"}
  {"impressionID": "100004", "ImpressionTime": "2020-11-01 10:46:00" , "CampaignName": "AG India"}
   */
  val impressionSchema = StructType(List(
    StructField("impressionID", StringType, true),
    StructField("ImpressionTime", TimestampType, true),
    StructField("CampaignName", StringType, true)
  ))

  /*
  {"clickID": "100001", "ClickTime": "2020-11-01 10:15:00"}
  {"clickID": "100001", "ClickTime": "2020-11-01 10:16:00"}
  {"clickID": "100003", "ClickTime": "2020-11-01 10:50:00"}
  {"clickID": "100004", "ClickTime": "2020-11-01 11:00:00"}
  */
  val clickSchema = StructType(List(
    StructField("clickID", StringType, true),
    StructField("ClickTime", TimestampType, true)
  ))

  //Reading from a stream source
  val rawImpressionDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 1722)
    .load()

  val rawClickDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 1712)
    .load()

  /*
  Note: If we don't use "withWatermark()" then the state store of the dataframes will keep growing and we will get memory issues
  withWatermark will help us to clean the state store
  watermark boundary = max(eventTime) - delayThreshold
   */

  val tempImpressionDF = rawImpressionDF.select(from_json(col("value"), impressionSchema).as("value"))
  val impressionDF = tempImpressionDF.select("value.*").withWatermark(eventTime = "ImpressionTime", delayThreshold = "30 minute")

  val tempClickDF = rawClickDF.select(from_json(col("value"), clickSchema).as("value"))
  val clickDF = tempClickDF.select("value.*").withWatermark(eventTime = "ClickTime", delayThreshold = "30 minute")

  //Join condition
  val joinExpr = expr("impressionID === clickID AND ClickTime BETWEEN ImpressionTime AND ImpressionTime + interval 15 minutes")

  //Join Type
  val joinType = "leftOuter"
  /*
    inner Join - Supported and recommended to use withWatermark for state store cleaning.
    leftOuter  - 1. There should be watermark on the 'right' side stream table/DF (better to use for both streams)
                 2. Maximum time constraint between left and the right side stream (check Join condition)
                    ex: If an impression happen at 10 pm then click on that before 10.15 pm will only be considered
    rightOuter - 1. There should be watermark on the 'left' side stream table/DF (better to use for both streams)
                 2. Maximum time constraint between left and the right side stream (check Join condition)

    ** Note: for left/right outer join if there is no match found within the watermark boundary then
      it will emit with nulls on left/right side columns once time is elapsed as it has to sow at least once
      and state store will be cleared
   */

  //Join 2 stream DF
  val joinedDF = impressionDF.join(clickDF, joinExpr, joinType).drop(clickDF.col("clickID"))

  //Output to Sink
  val outputQuery = joinedDF.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "checkpointDir")
    .trigger(Trigger.ProcessingTime("15 seconds"))
    .start()

  outputQuery.awaitTermination()

}
