package com.daimler.cc

import java.sql.Date

import com.daimler.cc.exampleKafkaToKafka.{logger, readFromKafka, spark, ssc, timestamp, toStock}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.dstream.DStream

import scala.util.{Failure, Success, Try}
import org.json4s.jackson.JsonMethods._
import com.daimler.common._

object KafkaToHive extends Serializable {
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("Spark Kafka To Hive")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  case class log(
                    timestamp: String,
                    data: String,
                    error: String
                  )
  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",//"earliest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "kafkalogger"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )
    val processedStream = kafkaDStream.map(record => record.value())
    processedStream
  }



  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("ERROR")

    val stream = readFromKafka()

    import spark.implicits._

    val ds:DStream[log] = stream.filter(line=>line.contains("data")).map { line =>
        val jsonMap = parse(line).values.asInstanceOf[Map[String, String]]
        log(jsonMap("timestamp"), jsonMap("data"), jsonMap("error"))
      }

    import spark.sql

    sql("CREATE DATABASE IF NOT EXISTS Ro45")
    sql("USE Ro45")

     ds.foreachRDD { rdd =>
      val df = spark.createDataFrame(rdd)
       df.write
         .mode(SaveMode.Append)
         .format("csv")
         //.partitionBy("ORIGIN", "OP_CARRIER")
         .saveAsTable("Ro45.log_data")
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
