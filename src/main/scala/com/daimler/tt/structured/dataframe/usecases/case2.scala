package com.daimler.tt.structured.dataframe.usecases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/* Week 12 -  DataFrames Session 23, 24
Use case:
  1. To read log file and group by level, month and count the log levels in each month
  2. Create a Pivot log Table (level as rows, month as columns and count as cells
* */
case class Logging(level:String, datetime:String)

object case2 {
  def main(args: Array[String]): Unit = {

    def mapper(line:String):Logging ={
      val fields = line.split(",")
      Logging(fields(0).trim, fields(1).trim)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "Spark SQL Example")
    sparkConf.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val myLogList = List("WARN, 2016-12-31 04:17:32",
      "FATAL, 2016-12-31 03:10:32",
      "WARN, 2016-12-31 07:59:32",
      "INFO, 2015-5-21 09:19:32",
      "FATAL, 2015-4-11 04:39:32")

    import spark.implicits._

    val baseRDD = spark.sparkContext.parallelize(myLogList)

    val rdd = baseRDD.map(mapper)

    val baseDF = rdd.toDF()

    baseDF.createOrReplaceTempView("logs")

    //spark.sql("select level, collect_list(datetime) from logs group by level order by level").show(false)
    //spark.sql("select level, count(datetime) from logs group by level order by level").show(false)
    val logDF = spark.sql("select level, date_format(datetime,'MMMM') as month from logs")

    logDF.createOrReplaceTempView("logs_table")

    spark.sql("select level, month, count(1) as count from logs_table group by level, month").show(false)

    val baseFileDF = spark.read.format("csv").option("header", true).option("path","input/biglog.txt").load()

    baseFileDF.createOrReplaceTempView("logs_file_table")

    /*
    * date_format(datetime,'MMMM') as month => To get Month in Alphabets (January, February....)
    * date_format(datetime,'M')             => To get Number of the month (1,2 .....12)
    * first(date_format(datetime,'M'))      => As this is not used in group by we need to do some aggregation (or else it will throw error)
    *                                          So first will give only first value in the grouped values
    * cast(first(date_format(datetime,'M')) as int) => we are converting the month number into Integer format to sort by month
    * */
    spark.sql("""select level,
                |date_format(datetime,'MMMM') as month,
                |cast(first(date_format(datetime,'M')) as int) as month_num,
                |count(1) as count from logs_file_table
                |group by level, month order by month_num, level""".stripMargin)
                .drop("month_num").show(false)

    //Pivot log Table (level as rows and month as columns
    spark.sql("""select level,
                |cast(date_format(datetime,'M') as int) as month_num
                |from logs_file_table""".stripMargin)
              .groupBy("level").pivot("month_num").count().show()

    //A bit optimisation by providing pivot column values instead system calculating every time
    val monthList = List("January","February","March","April","May","June","July","August","September","October","November","December")

    spark.sql("""select level,
                |date_format(datetime,'MMMM') as month,
                |from logs_file_table""".stripMargin)
      .groupBy("level").pivot("month",monthList).count().show()



    spark.stop()

  }
}
