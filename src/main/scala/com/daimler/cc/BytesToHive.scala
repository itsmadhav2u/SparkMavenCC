package com.daimler.cc
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.json4s.jackson.JsonMethods.mapper

object BytesToHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Kafka To Hive")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.sql
    import spark.implicits._

    val someDF = Seq(("Arpitha"),("madhav"),("viswa"),("bijay")).toDF("names")
    someDF.show()

    val getByteArray = udf((name: String) => mapper.writeValueAsBytes(name))

    val df = someDF.withColumn("bytearray", getByteArray($"names"))
    df.printSchema()
    df.show(false)
    df.write
      .mode(SaveMode.Overwrite)
      //.partitionBy("ORIGIN", "OP_CARRIER")
      .saveAsTable("Ro45.byteArray")


    sql("use ro45")
    val readDF = sql("select * from  byteArray")
    readDF.printSchema()
    readDF.show(false)

    val getString = udf((name: Array[Byte]) => new String(name))
    val df3 = readDF.withColumn("newcol", getString(col("bytearray")))
    df3.show(false)
    df3.printSchema()
    readDF.registerTempTable("temp_DF")
    val dfnew=spark.sql("SELECT CONCAT(names, ' ', bytearray) FROM temp_DF")
    dfnew.show(false)
  }


}
