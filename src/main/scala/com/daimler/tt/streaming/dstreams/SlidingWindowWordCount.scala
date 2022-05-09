package com.daimler.tt.streaming.dstreams

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SlidingWindowWordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Word count (State Full)")

  val ssc = new StreamingContext(sc, Seconds(2))

  ssc.checkpoint("checkPointDir")

  val dStream = ssc.socketTextStream("localhost", 1722)

  val wordsDstream = dStream.flatMap(line => line.split(" "))

  val pairedDstream = wordsDstream.map(word => (word, 1))

  /*
  reduceByKeyAndWindow - State full transformation but considers state of window size
  reduceByKeyAndWindow(Summery Func, Inverse Func, Window Size, Sliding Interval)
  and it requires a Dstream of paired RDD
  */
  val outputDstream = pairedDstream.reduceByKeyAndWindow((x,y) => x+y, (x,y) => x-y, Seconds(10), Seconds(4))
                                    .filter(pair => pair._2 > 0) //To filter the words which are 0 occurrences during window period

  /*
  Important Note:  Window Size and Sliding Interval should be integral multiple of batch interval
  -> we will see very first result after 10 seconds (once at least one window is complete)
  -> second results after 14 seconds ( 10 + 4 (sliding Interval)) and next 18, 22, 26 ( i.e +4)
  -> result will be calculated only for last 5 RDD's or last 10 seconds data
  */

  outputDstream.print()

  ssc.start()

  ssc.awaitTermination()
}
