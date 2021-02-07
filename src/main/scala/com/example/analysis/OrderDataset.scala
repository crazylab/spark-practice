package com.example.analysis

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object OrderDataset {

  val spark = SparkSession.builder()
    .appName("Order Data")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val employees = List(
      Employee(id = 1, name = "John", salary = 5000, department = "sales", gender = "M"),
      Employee(id = 2, name = "Jacob", salary = 10000, department = "HR", gender = "M"),
      Employee(id = 3, name = "Julia", salary = 1000, department = "sales", gender = "F"),
      Employee(id = 4, name = "Jackson", salary = 25000, department = "marketing", gender = "M"),
      Employee(id = 5, name = "Anna", salary = 8000, department = "HR", gender = "F"),
      Employee(id = 6, name = "Linda", salary = 12000, department = "HR", gender = "F"),
      Employee(id = 7, name = "Devid", salary = 5000, department = "marketing", gender = "M"),
      Employee(id = 8, name = "Peter", salary = 4000, department = "sales", gender = "M"),
      Employee(id = 9, name = "Bob", salary = 3000, department = "marketing", gender = "M"),
      Employee(id = 10, name = "Robin", salary = 7000, department = "marketing", gender = "F")
    ).toDF.cache()

    employeesOrderedBySalaryDescNameAscending(employees)
    employeesRankedBasedOnSalary(employees)
  }

  def employeesOrderedBySalaryDescNameAscending(records: DataFrame): Unit = {
    import spark.implicits._
    val orderedRecords = records.orderBy($"salary".desc, $"name".asc)
    val firstRecord = orderedRecords.as[Employee].first()

    val jackson = Employee(id = 4, name = "Jackson", salary = 25000, department = "marketing", gender = "M")
    assert(jackson == firstRecord, "Employees ordered by salary desc and name asc did not match")
  }

  def employeesRankedBasedOnSalary(records: DataFrame): Unit = {
    val windowSpec = Window.orderBy(col("salary").desc)
    val recordsWithRank = records.withColumn("rank", dense_rank().over(windowSpec))
    val firstRecord = recordsWithRank.first()

    assert(firstRecord.getAs[Int]("rank") == 1, "Ranking did not match")
  }
}
