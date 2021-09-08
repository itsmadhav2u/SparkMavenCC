package com.daimler.cc

object ParseFunc {
  def quantityWithUnitToUnit(s: String): Option[String] = {
    try {
      Some(s.split(' ')(1).trim)
    } catch {
      case _: Exception => None
    }
  }
  def amountWithUnitToUnit(s: String): Option[String] = {
    try {
      Some(s.split(' ')(1).trim)
    } catch {
      case _: Exception => None
    }
  }
  def amountWithUnitToAmount(s: String): Option[java.math.BigDecimal] = {
    try {
      s match {
        case x if x.contains(' ') => toAmount(x.split(' ')(0))
        case x => toAmount(x)
      }
    } catch {
      case _: Exception => None
    }
  }
  def toAmount(s: String): Option[java.math.BigDecimal] = {
    try {
      Some(new java.math.BigDecimal(s.trim match {
        case x => x.replaceAll("\\.", "").replaceAll(",", ".")
      }))
    } catch {
      case _: Exception => None
    }
  }
  def quantityWithUnitToQuantity(s: String): Option[java.lang.Integer] = {
    try {
      s match {
        case x if x.contains(' ') => toInteger(x.split(' ')(0))
        case x => toInteger(x)
      }
    } catch {
      case _: Exception => None
    }
  }
  def toInteger(s: String): Option[java.lang.Integer] = {
    try {
      Some(
        s.trim
          .replace(",", ".")
          .split('.')(0)
          .toInt
      )
    } catch {
      case _: Exception => None
    }
  }

}
