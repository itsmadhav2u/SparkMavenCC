package com.daimler.cc
import com.daimler.common._
import java.sql.Date
import java.text.SimpleDateFormat
import scala.util.{Try,Success,Failure}

object exampleScala {

  def main(args: Array[String]): Unit = {

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
        case Failure(ex) => printException(s"Invalid data $line and got exception as $ex")
      }
    })


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
