package com.daimler.cc

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, StringType, StructField, StructType}
import ParseFunc._

object nicos extends App {

  /**
   * Schema information about the used vehicle Kafka message
   */
  val UsedVehicle =
    StructType(
      Array(
        StructField("Kostenrechnungskreis", StringType, nullable = true), //Kostenrechnungskreis
        StructField("T_Kostenrechnungskreis", StringType, nullable = true), //T_Kostenrechnungskreis
        StructField("Vertriebskanal", StringType, nullable = true), //Vertriebskanal
        StructField("T_Vertriebskanal", StringType, nullable = true), //T_Vertriebskanal(StructField("Verbund", StringType, true), 4),                    //>Verbund
        StructField("Verbund", StringType, nullable = true), //>Verbund
        StructField("T_Verbund", StringType, nullable = true), //T_>Verbund
        StructField("Umsatzgebiet", StringType, nullable = true), //Umsatzgebiet
        StructField("T_Umsatzgebiet", StringType, nullable = true), //T_Umsatzgebiet
        StructField("FahrgestellnrVIN", StringType, nullable = true), //Fahrgestellnr (VIN)
        StructField("Baumuster", StringType, nullable = true), //Baumuster
        StructField("T_Baumuster", StringType, nullable = true), //T_Baumuster
        StructField("Aufbau", StringType, nullable = true), //Aufbau
        StructField("T_Aufbau", StringType, nullable = true), //T_Aufbau
        StructField("Baureihe", StringType, nullable = true), //Baureihe
        StructField("T_Baureihe", StringType, nullable = true), //T_Baureihe
        StructField("Untersparte", StringType, nullable = true), //>Untersparte
        StructField("T_Untersparte", StringType, nullable = true), //T_>Untersparte
        StructField("Fahrzeuggruppe", StringType, nullable = true), //>Fahrzeuggruppe
        StructField("T_Fahrzeuggruppe", StringType, nullable = true), //T_>Fahrzeuggruppe
        StructField("Buchungsdatum", DateType, nullable = true), //Buchungsdatum
        StructField("SparteAG", StringType, nullable = true), //Sparte AG
        StructField("T_SparteAG", StringType, nullable = true), //T_Sparte AG
        StructField("Finanzierungsart", StringType, nullable = true), //Finanzierungsart
        StructField("T_Finanzierungsart", StringType, nullable = true), //T_Finanzierungsart
        StructField("HGart", StringType, nullable = true), //>H-Gart
        StructField("T_HGart", StringType, nullable = true), //T_>H-Gart
        StructField("Geschaeftsart", StringType, nullable = true), //Geschaeftsart
        StructField("T_Geschaeftsart", StringType, nullable = true), //T_Geschaeftsart
        StructField("AntriebTonnage", StringType, nullable = true), //>Antrieb / Tonnage
        StructField("T_AntriebTonnage", StringType, nullable = true), //T_>Antrieb / Tonnage
        StructField("AuftragsnummerVAF", StringType, nullable = true), //Auftragsnummer VAF
        StructField("Verkaeufer", StringType, nullable = true), //Verkäufer
        StructField("KzDrehscheibengesc", StringType, nullable = true), //Kz. Drehscheibengesc
        StructField("T_KzDrehscheibengesc", StringType, nullable = true), //T_Kz. Drehscheibengesc
        StructField("JungeSterne", StringType, nullable = true), //Junge Sterne
        StructField("T_JungeSterne", StringType, nullable = true), //T_Junge Sterne
        StructField("LaufleistungLtZaeh", IntegerType, nullable = true), //Laufleistung lt. Zäh
        StructField("Hereinnahme", StringType, nullable = true), //>Hereinnahme (YYYYMMM)
        StructField("GfasauftrGisEzla", DateType, nullable = true), //GFASAUFTR__GIS_EZLA
        StructField("Absatz", IntegerType, nullable = true), //Absatz
        StructField("UNIT_Absatz", StringType, nullable = true), //UNIT Absatz
        StructField("Umsatz", DecimalType(9, 2), nullable = true), //Umsatz
        StructField("CURR_Umsatz", StringType, nullable = true), //Währung Umsatz
        StructField("TIMESTAMP", StringType, nullable = true) //TIMESTAMP
      ))


  /**
   * Schema information about the used vehicle CSV file
   */
  val UsedVehicleCSV =
    StructType(
      Array(
        StructField("Kostenrechnungskreis", StringType, nullable = true), //Kostenrechnungskreis
        StructField("T_Kostenrechnungskreis", StringType, nullable = true), //T_Kostenrechnungskreis
        StructField("Vertriebskanal", StringType, nullable = true), //Vertriebskanal
        StructField("T_Vertriebskanal", StringType, nullable = true), //T_Vertriebskanal(StructField("Verbund", StringType, true), 4),                    //>Verbund
        StructField("Verbund", StringType, nullable = true), //>Verbund
        StructField("T_Verbund", StringType, nullable = true), //T_>Verbund
        StructField("Umsatzgebiet", StringType, nullable = true), //Umsatzgebiet
        StructField("T_Umsatzgebiet", StringType, nullable = true), //T_Umsatzgebiet
        StructField("FahrgestellnrVIN", StringType, nullable = true), //Fahrgestellnr (VIN)
        StructField("Baumuster", StringType, nullable = true), //Baumuster
        StructField("T_Baumuster", StringType, nullable = true), //T_Baumuster
        StructField("Aufbau", StringType, nullable = true), //Aufbau
        StructField("T_Aufbau", StringType, nullable = true), //T_Aufbau
        StructField("Baureihe", StringType, nullable = true), //Baureihe
        StructField("T_Baureihe", StringType, nullable = true), //T_Baureihe
        StructField("Untersparte", StringType, nullable = true), //>Untersparte
        StructField("T_Untersparte", StringType, nullable = true), //T_>Untersparte
        StructField("Fahrzeuggruppe", StringType, nullable = true), //>Fahrzeuggruppe
        StructField("T_Fahrzeuggruppe", StringType, nullable = true), //T_>Fahrzeuggruppe
        StructField("Buchungsdatum", StringType, nullable = true), //Buchungsdatum
        StructField("SparteAG", StringType, nullable = true), //Sparte AG
        StructField("T_SparteAG", StringType, nullable = true), //T_Sparte AG
        StructField("Finanzierungsart", StringType, nullable = true), //Finanzierungsart
        StructField("T_Finanzierungsart", StringType, nullable = true), //T_Finanzierungsart
        StructField("HGart", StringType, nullable = true), //>H-Gart
        StructField("T_HGart", StringType, nullable = true), //T_>H-Gart
        StructField("Geschaeftsart", StringType, nullable = true), //Geschaeftsart
        StructField("T_Geschaeftsart", StringType, nullable = true), //T_Geschaeftsart
        StructField("AntriebTonnage", StringType, nullable = true), //>Antrieb / Tonnage
        StructField("T_AntriebTonnage", StringType, nullable = true), //T_>Antrieb / Tonnage
        StructField("AuftragsnummerVAF", StringType, nullable = true), //Auftragsnummer VAF
        StructField("Verkaeufer", StringType, nullable = true), //Verkäufer
        StructField("KzDrehscheibengesc", StringType, nullable = true), //Kz. Drehscheibengesc
        StructField("T_KzDrehscheibengesc", StringType, nullable = true), //T_Kz. Drehscheibengesc
        StructField("JungeSterne", StringType, nullable = true), //Junge Sterne
        StructField("T_JungeSterne", StringType, nullable = true), //T_Junge Sterne
        StructField("LaufleistungLtZaeh", StringType, nullable = true), //Laufleistung lt. Zäh
        StructField("Hereinnahme", StringType, nullable = true), //>Hereinnahme (MMM.YYYY)
        StructField("GfasauftrGisEzla", StringType, nullable = true), //GFASAUFTR__GIS_EZLA
        StructField("Absatz", StringType, nullable = true), //Absatz
        StructField("Umsatz", StringType, nullable = true), //Umsatz
        StructField("TIMESTAMP", StringType, nullable = true) //TIMESTAMP
      ))

  def getSourceIndex(schema: StructType, name: String): Int = {
    val foundField = schema.collect { case (field) if field.name.equals(name) => field }.head
    schema.indexOf(foundField)
  }

  def transform(sourceSchema: StructType, data: Row, targetSchema: StructType): Seq[Any] = {
    targetSchema.map(fieldinfo => {
      fieldinfo.name match {
        case "Absatz" => ParseFunc.quantityWithUnitToQuantity(data.getAs[String](getSourceIndex(sourceSchema, "Absatz"))).orNull
        case "UNIT_Absatz" => ParseFunc.quantityWithUnitToUnit(data.getAs[String](getSourceIndex(sourceSchema, "Absatz"))).getOrElse("")
        case "Umsatz" => ParseFunc.amountWithUnitToAmount(data.getAs[String](getSourceIndex(sourceSchema, "Umsatz"))).orNull
        case "CURR_Umsatz" => ParseFunc.amountWithUnitToUnit(data.getAs[String](getSourceIndex(sourceSchema, "Umsatz"))).getOrElse("")
        case x => data.getAs[String](getSourceIndex(sourceSchema, x)).trim
      }
    })
  }


  val spark = SparkSession.builder()
    .appName("Spark Kafka To Hive")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val myString =
    """
      |0076;MBVD;100;Retail NDL;NLP01;PKW Nord;205000;LÃ¼beck;TTB1569421J481722;156942;GLA 180;SUV;Sport Utility Vehicl;156;156 GLA;100;A-Klasse;#;Nicht zugeordnet;05.07.2019;10;PKW;#;keine Finanzierung;10;Gebrauchtfahrzeuge;108;GJH JawaHerstLeas;BENZ;BENZIN;09205GX056;1J48;N;Eigenware;2;Junger Stern Her+Ver;9971;003.2019;10.01.2018;1,000;23.739,49;20190708040023
      |""".stripMargin
  val data = org.apache.spark.sql.Row.fromSeq(myString.split(';'))
  println(data)
  println(getSourceIndex(UsedVehicleCSV, "Absatz"))

  println(data.getAs[String](getSourceIndex(UsedVehicleCSV, "Absatz")))
  println(data.getAs[String](39))
  println(data.getAs[String](getSourceIndex(UsedVehicleCSV, "Umsatz")))

  println("madhav rohit".split(' ')(1).trim)

  println(quantityWithUnitToUnit("1,000"))
  println(amountWithUnitToUnit("23.739,49"))

  println("-",quantityWithUnitToUnit("1,000").getOrElse(""))
  println('-',amountWithUnitToUnit("23.739,49").getOrElse(""))

  //val newData = transform(UsedVehicleCSV, data, UsedVehicle)
 // println(newData)
}
