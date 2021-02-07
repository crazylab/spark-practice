package com.example.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Practice {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("New App")
      .master("local[*]")
      .getOrCreate()

    problem1(spark)
  }

  def problem1(spark: SparkSession): Unit = {
    // Find the sum of column c5 grouped by c1, c2, c3, c4
    // Also add one column to show how many columns got grouped into
    import spark.implicits._
    val data = List(
      (1, 2, 3, 4, 20),
      (1, 2, 3, 4, 10),
      (1, 2, 3, 4, 30),
      (5, 6, 7, 8, 10),
      (5, 6, 7, 8, 20),
    ).toDF("c1", "c2", "c3", "c4", "c5")

    val expected = List(
      (1, 2, 3, 4, 60, 3),
      (5, 6, 7, 8, 30, 2),
    ).toDF("c1", "c2", "c3", "c4", "c5_sum", "num_rows_grouped")


    val result = data
      .groupBy("c1", "c2", "c3", "c4")
      .agg(
        sum("c5").as("c5_sum"),
        count("c1").as("num_rows_grouped")
      )

    assert(result.exceptAll(expected).count() == 0)
  }

}
