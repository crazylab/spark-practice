package com.example.joins

import com.example.analysis.Employee
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinDatasets {
  // Inner join
  //  left outer join
  //  right outer join
  //  cross join
  //  broadcast join
  //

  val spark = SparkSession.builder()
    .appName("Join data")
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
    ).toDF
      .repartition(3)
      .cache()

    employees.groupBy("gender").agg(round(avg("salary"),2).as("avg"))
      .show()
  }

}
