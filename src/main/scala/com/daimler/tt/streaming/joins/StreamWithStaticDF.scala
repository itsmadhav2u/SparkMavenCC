package com.daimler.tt.streaming.joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
//week 16 - session 11,12
object StreamWithStaticDF extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Stream and Static DF Join")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  val transactionsSchema = StructType(List(
    StructField("card_id", LongType, true),
    StructField("amount", IntegerType, true),
    StructField("postcode", IntegerType, true),
    StructField("pos_id", LongType, true),
    StructField("transaction_dt", TimestampType, true)
  ))

  /* Reading from source
  Sample data
  {"card_id":5572427538311236,"amount":356492,"postcode":53149,"pos_id":160065694733720,"transaction_dt":"2018-09-22 05:21:43"}
  {"card_id":5134334388575160,"amount":8241105,"postcode":13302,"pos_id":338546916831311,"transaction_dt":"2018-02-12 19:25:59"}
   */
  val rawStreamingDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 1722)
    .load()

  val tempTransactionsDF = rawStreamingDF.select(from_json(col("value"), transactionsSchema).as("value"))

  val transactionsDF = tempTransactionsDF.select("value.*")

  /* Loading Static DataFrame
  Sample Data
  card_id,member_id,card_issue_date,country,state
  5572427538311236,976740397894598,2014-12-15 08:06:58.0,United States,Tonawanda
  */
  val membersDF = spark.read.format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "input/streamJoinTableStatic.txt")
    .load()

  val joinCondition = transactionsDF.col("card_id") === membersDF.col("card_id")

  /* Stream DF Join with Static DF
  inner join  -  possible
  left outer  - Possible only if left side table is streaming DF and right side table is static DF or else Error
  right outer - Possible only if right side table is streaming DF and left side table is static DF or else Error
  full other  - Not supported
   */

  val joinType = "inner"

  //Joining DF's and dropping duplicate column to avoid confusion
  val enrichedDF = transactionsDF.join(membersDF, joinCondition, joinType).drop(membersDF.col("card_id"))

  //Write to sink
  val joinQuery = enrichedDF.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "checkpointDir")
    .trigger(Trigger.ProcessingTime("15 seconds"))
    .start()

  joinQuery.awaitTermination()

}
