package com.daimler.tt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MaxSpendByCust extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Finding Max spent by customer")

  val baseRDD = sc.textFile("input/customerorders-201008-180523.csv")
  //Data: customer_id, product_id, amount_spent

  val mappedRDD = baseRDD.map(line => line.split(",")).map(pair => (pair(0), pair(2).toFloat))

  val totalByCustRDD = mappedRDD.reduceByKey((a, b) => a + b)

 // val outputRDD = totalByCustRDD.sortBy(x => x._2, false)

  //totalByCustRDD.take(5).foreach(println)

  println("--------------------------------------")

  val premiumCustRDD = totalByCustRDD.filter(pair => pair._2 > 5000).cache()
  //We can use persist as well instead of cache

  premiumCustRDD.take(5).foreach(println)

  println("--------------------------------------")

  println(premiumCustRDD.count)

  scala.io.StdIn.readLine()
}
