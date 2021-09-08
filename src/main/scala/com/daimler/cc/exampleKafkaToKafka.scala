package com.daimler.cc
import java.sql.Date
import java.text.SimpleDateFormat
import scala.util.{Try,Success,Failure}
import com.daimler.common._
import org.apache.log4j.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object exampleKafkaToKafka {

  lazy val logger : Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "invoices"

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

  val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)

  val dateFormat = new SimpleDateFormat("MMM d yyyy")

  def toStock(line:String)={
    val tokens = line.split(",")
    val company = tokens(0)
    val date = new Date(dateFormat.parse(tokens(1)).getTime)
    val price = tokens(2).toDouble

    Stock(company, date, price)
  }

  def main(args: Array[String]): Unit = {

    //spark.sparkContext.setLogLevel("ERROR")
    //logger.error("Newly started")
    logger.debug("Debug message from HelloKafkaLogger.main,")
    logger.info("Info message from HelloKafkaLogger.main")
    logger.warn("Warn message from HelloKafkaLogger.main")

    val stream = readFromKafka()

     stream.foreachRDD(rdd =>
      rdd.foreachPartition { partition =>
        partition.foreach(line =>{
      val stock = Try(toStock(line))
      stock match {
        case Success(stockObject) => println( "Valida data " + stockObject)
        case Failure(ex) => {
          //println(s"""{data: "$line",error:$ex}""")
          //logger.error(s"Invalid data $line and got exception as $ex")
          //logger.error(s"""{timestamp: "$timestamp" data: "$line",error:$ex}""")
          logger.error(s"""timestamp: "$timestamp" data: "$line", error: "${ex.toString.replace('"',''')}"""")
        }}
    })
  })
    ssc.start()
    ssc.awaitTermination()
  }
}
