package com.daimler.tt.streaming.dstreams

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}


//Dstream word count (State Full)

object StateFullWordCount extends App {

  def updateStateFunc(newValues:Seq[Int], previousState:Option[Int]) :Some[Int] ={
    val newCount = previousState.getOrElse(0) + newValues.sum
    Some(newCount)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Word count (State Full)")

  val ssc = new StreamingContext(sc, Seconds(5))

  ssc.checkpoint("checkPointDir")

  val dStream = ssc.socketTextStream("localhost", 1722)

  val wordsDstream = dStream.flatMap(line => line.split(" "))

  val pairedDstream = wordsDstream.map(word => (word, 1))

  //updateStateByKey : is a state full transformation (considers entire Dstream)
  val outputDStream = pairedDstream.updateStateByKey(updateStateFunc)

  outputDStream.print()

  ssc.start()

  ssc.awaitTermination()

}
