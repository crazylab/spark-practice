package com.example.io

import org.apache.spark.sql.SparkSession

class ReadWriteDB {
  //  TODO: Need to do this
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Read Hive Catalog")
      .master("local[*]")
      .getOrCreate()

  }
}
