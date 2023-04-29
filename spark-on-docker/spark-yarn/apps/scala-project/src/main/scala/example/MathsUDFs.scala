package com.myudfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction


object MathUDFs {

  def isGreaterThanZero(columnValue: Int): Boolean = {
      columnValue > 0
  }

  val isGreaterThanZeroUDF: UserDefinedFunction = udf(isGreaterThanZero _ )

  def multiplyBy(multiplier: Int) = {
    udf((columnValue: Int) => {
      columnValue * multiplier
    })
  }

  def registerUdf: UserDefinedFunction = {
  		val spark = SparkSession.builder().getOrCreate()
  		spark.udf.register("isGreaterThanZero", (columnValue: Int) => isGreaterThanZero(columnValue))
  }

}
