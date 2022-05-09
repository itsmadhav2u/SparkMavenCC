package com.daimler.tt.streaming.dstreams

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CountByWindowExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Count lines in a Dstream")

  val ssc = new StreamingContext(sc, Seconds(2))

  ssc.checkpoint("checkPointDir")

  val dStream = ssc.socketTextStream("localhost", 1722)

  //countByWindow : count the lines in a Dstream during a specified window period
  //countByWindow(Window Size, Sliding Interval)
  val outputDstream = dStream.countByWindow(Seconds(10), Seconds(4))

  outputDstream.print()

  ssc.start()

  ssc.awaitTermination()
}
