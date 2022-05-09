package com.daimler.tt.streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

//week 15 session 2,3,4
object WordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Structured Streaming Word Count")
                                    .master("local[*]")
                                    .config("spark.sql.shuffle.partitions", 2)
                                    .config("spark.streaming.stopGracefullyOnShutdown", "true")
                                    .getOrCreate()

  //Reading a stream data from a source
  val linesDF = spark.readStream.format("socket")
                      .option("host", "localhost")
                      .option("port", 1722)
                      .load()

  linesDF.printSchema()

  //Processing Stream data
  val wordsDF = linesDF.selectExpr("explode(split(value, ' ')) as words")
  val countDF = wordsDF.groupBy("words").count()

  //Writing Stream data to sink
  /*
  DataStreamWriter allows 3 output modes
    1. append    - prints only new rows
        ** Note: when we do aggregations append mode is not allowed and gives Analysis Exception if we use
    2. update    - prints only updated/new rows i.e upserts
    3. complete  - prints all rows
  */
  val wordCountQuery: StreamingQuery = countDF.writeStream.format("console")
                                              .outputMode("complete")
                                              .option("checkpointLocation", "checkPointDir")
                                              .trigger(Trigger.ProcessingTime("17 seconds"))
                                              .start()

  /*
  start() => acts like a action which will trigger the execution. For every micro batch a new job will be created.
  i.e spark driver will take the code from readStream to writeStream and submits it to spark sql engine.
  now sql engine will analyze, optimize and then compile it to generate the execution plan (stages and tasks)
  then spark will start a background thread/process to execute it, and this thread will start a new job for every
  micro batch to repeat the processing and writing operations.
  this loop is created and managed by background process/thread.
   */
  /*
  Data Stream Writer will allow us to define the trigger configurations for micro batch creation.
  1. unspecified (default) -> new micro batch created as soon as current batch ends but it will wait for new input data.
  2. time interval -> ex: 2 mins. First micro batch starts immediately but second micro batch starts when first batch processed
                      and time interval is elapsed and also new data available. If first batch is not finished in 2 mins then
                      it will wait for it to complete and then immediately it will start processing for next micro batch.
  3. one time -> Similar to batch processing but this will keep track of what processed earlier and process only new data
                  i.e it will maintain previous state and data already processed.
  4. continuous -> processes data as soon as data arrives with milli seconds latency.
  */

  wordCountQuery.awaitTermination()
}
