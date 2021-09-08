package com.daimler.cc

import java.sql.Date
import java.text.SimpleDateFormat
import scala.util.{Try,Success,Failure}
import com.daimler.common._
import org.apache.log4j.Logger

object exampleSendToKafka {
  def main(args: Array[String]): Unit = {
    lazy val logger : Logger = Logger.getLogger("kafka")

    logger.warn("started")
    logger.debug("Debug message from HelloKafkaLogger.main,")
    logger.info("Info message from HelloKafkaLogger.main")
    logger.warn("Warn message from HelloKafkaLogger.main")

    val lines = Q(
      """
        |AAPL,Sep 1 2000,1$2.88
        |AAPL,Oct 1 ,9.78
        |AAPL,Nov 1 2000,8.25
        |AAPL,Dec 1 2000,abc
        |AAPL,ro45 1 2001,10.81
        |AAPL,Feb 1 2001,9.12
        |""".stripMargin)


    lines.foreach(line =>{
      val stock = Try(toStock(line))
      stock match {
        case Success(stockObject) => println( "Valida data " + stockObject)
        case Failure(ex) => {
          //logger.error(s"Invalid data $line and got exception as $ex")
          logger.error(s"""{data: "$line",error:$ex}""")
        }
      }
    })

    logger.warn(" Ended")


  }

  def Q(s: String): Seq[String] = s.split("\n").toSeq.map(_.trim).filter(_ != "")

  val dateFormat = new SimpleDateFormat("MMM d yyyy")

  def toStock(line:String)={
    val tokens = line.split(",")
    val company = tokens(0)
    val date = new Date(dateFormat.parse(tokens(1)).getTime)
    val price = tokens(2).toDouble

    Stock(company, date, price)
  }

  def printException(s:String) = println(s)

}
