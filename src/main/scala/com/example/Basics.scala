package com.example

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Basics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("New App")
      .master("local[*]")
      .getOrCreate()

    val numbersDF = createDataFrame(spark)

    // Get the analysed plan in the tree structure
    numbersDF.queryExecution.analyzed.numberedTreeString
  }

  def createDataFrame(sparkSession: SparkSession): DataFrame = {
    val data = (1 to 10).map(value => {
      Row.fromSeq(Array(value, value + 1, value + 2))
    })
    val dataRDD = sparkSession.sparkContext.makeRDD(data)
    val schema = StructType(Array("n", "n+1", "n+2").map(name => {
      StructField(name, IntegerType)
    }))

    sparkSession.createDataFrame(dataRDD, schema)
  }

}
