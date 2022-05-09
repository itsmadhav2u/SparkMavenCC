package com.daimler.tt.streaming.dstreams

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

//Streaming word count Example (State Less)

object DstreamWordCount extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf =  new SparkConf()
    sparkConf.set("spark.app.name", "Dstream Word Count")
    sparkConf.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))

    val baseDstream = ssc.socketTextStream("localhost", 1722)

    val wordsDstream = baseDstream.flatMap(line => line.split(" "))

    val pairedDstream = wordsDstream.map(word => (word, 1))

    //reduceByKey : is a state less transformation
    val outputDstream = pairedDstream.reduceByKey((x,y) => x+y)

    outputDstream.print()

    ssc.start()

    ssc.awaitTermination()

}
