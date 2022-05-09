package com.daimler.tt.streaming.dstreams

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RunningTotal extends App{

  def summaryFunc(x:String, y:String):String ={
    (x.toInt + y.toInt).toString
  }

  def inverseFunc(x:String, y:String):String ={
    (x.toInt - y.toInt).toString
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Running Total (State Full)")

  val ssc = new StreamingContext(sc, Seconds(2))

  ssc.checkpoint("checkPointDir")

  val dStream = ssc.socketTextStream("localhost", 1722)

  val numDstream = dStream.flatMap(line => line.split(" ")).filter(num => num.trim != "")

  //reduceByWindow(Summery Func, Inverse Func, Window Size, Sliding Interval)
  val outputDstream = numDstream.reduceByWindow(summaryFunc(_, _), inverseFunc(_, _), Seconds(10), Seconds(4))

  outputDstream.print()

  ssc.start()

  ssc.awaitTermination()
}
