package com.myudfs

import org.apache.spark.sql.api.java.UDF1

// https://datanoon.com/blog/pyspark_udf/
// Here, the UDF1 class is imported because we are passing a single argument to the UDF.
// So, if the UDF is supposed to take 2 arguments, then UDF2 must be imported, similarly for 3 arguments, UDF3 and so on.

class LowerCaseString extends UDF1[String, String] {

  override def call(columnValue: String): String = {
    columnValue.toLowerCase()
  }
}
