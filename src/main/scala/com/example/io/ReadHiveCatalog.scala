package com.example.io

import org.apache.spark.sql.SparkSession

class ReadHiveCatalog {
//  https://jaceklaskowski.gitbooks.io/mastering-spark-sql/demo/demo-connecting-spark-sql-to-hive-metastore.html
//  TODO: Need to do this
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Read Hive Catalog")
      .master("local[*]")
      .getOrCreate()

//    spark.catalog.createTable()
  }
}
