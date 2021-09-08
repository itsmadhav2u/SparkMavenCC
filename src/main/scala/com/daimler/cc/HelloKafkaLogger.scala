package com.daimler.cc

import org.apache.log4j.Logger


object HelloKafkaLogger extends App {
  lazy val logger : Logger = Logger.getLogger("kafka")

  logger.debug("Debug message from HelloKafkaLogger.main,")
  logger.info("Info message from HelloKafkaLogger.main")
  logger.warn("Warn message from HelloKafkaLogger.main")
  //logger.error("Error message from HelloKafkaLogger.main")
  val a ="data"
  val e ="error "
  logger.error(a + " - "+ e)

}
