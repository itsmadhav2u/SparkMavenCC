package com.daimler.cc

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object kafkaBatch extends App{

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val kafkaDF: DataFrame = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "ag")
    .load()

  kafkaDF.printSchema()
  //kafkaDF.show(false)

  val filtered_df = kafkaDF
    .select(expr("cast(key as string) as key_string"),col("topic"), expr("cast(value as string) as Value"),
      col("partition"), col("offset"),col("partition").as("myPartition"))
  filtered_df.printSchema()
  filtered_df.show(false)

  println("Writing to Hive ")
  filtered_df.write
    .mode(SaveMode.Overwrite)
    .format("csv")
    .partitionBy("partition")
    //.bucketBy(4,"Value")
    .saveAsTable("ro45.kafka_hive")

  val dfs = spark.sql("select key_string,topic,Value,partition,offset,myPartition from ro45.kafka_hive")
  dfs.printSchema()
  dfs.show(false)
  println(dfs.count())

  println("final df")
  val finaldf = filtered_df.union(dfs).distinct()
  finaldf.show()
  println(finaldf.count())
}
