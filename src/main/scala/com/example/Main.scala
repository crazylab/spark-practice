package com.example

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Example")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val data = (1 to 50).toDS()
    data
      .filter(_ % 10 == 0)
      .foreach(println(_))
  }
}
