package com.example.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object FilterDataset {

  val spark = SparkSession.builder()
    .appName("Filer Dataset")
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

    findEmployeesWithNameStartingWithJ(employees)

    findEmployeeWithNameJulia(employees)

    findEmployeesWithSalaryGreaterThanEqualsTo5000(employees)

    findEmployeesWithSalaryLessThanEqualsTo5000(employees)

    findEmployeesWhoAreFemaleAndEarningMoreThan8000(employees)

    findEmployWithMinimumSalary(employees)

    findEmployeesWithMissingSalaryInformation(employees)
  }

  def findEmployeesWithNameStartingWithJ(dataset: DataFrame): Unit = {
    val filteredEmployees = dataset.where(col("name").startsWith("J"))
    assert(filteredEmployees.count() == 4, "Found more than 4 records for employ name starting with J")
  }

  def findEmployeeWithNameJulia(dataset: DataFrame): Unit = {
    import spark.implicits._
    val employ = dataset
      .where($"name" === "Julia")
      .as[Employee]
      .first()

    val julia = Employee(id = 3, name = "Julia", salary = 1000, department = "sales", gender = "F")
    assert(employ == julia, "The filtered employ didn't match")
  }

  def findEmployeesWithSalaryGreaterThanEqualsTo5000(records: DataFrame): Unit = {
    val filteredRecordCount = records.where(col("salary") >= 5000).count()

    assert(filteredRecordCount == 7, "Number of people who get salary >= 5000 didn't match")
  }

  def findEmployeesWithSalaryLessThanEqualsTo5000(records: DataFrame): Unit = {
    val filteredRecordCount = records.where(col("salary") <= 5000).count()

    assert(filteredRecordCount == 5, "Number of people who get salary <= 5000 didn't match")
  }

  def findEmployeesWhoAreFemaleAndEarningMoreThan8000(records: DataFrame): Unit = {
    import spark.implicits._
    val filteredRecordCount = records.where($"gender" === "F" and $"salary" > 8000).count()

    assert(filteredRecordCount == 1, "Number of female employ earning more than 8000 didn't match")
  }

  def findEmployWithMinimumSalary(records: DataFrame): Unit = {
    val minSalary = records
      .select(min("salary").as("minSalary"))
      .first()
      .getAs[Int]("minSalary")

    import spark.implicits._
    val employeeWithMinSalary = records
      .where($"salary" === minSalary)
      .as[Employee]
      .first()

    val julia = Employee(id = 3, name = "Julia", salary = 1000, department = "sales", gender = "F")
    assert(employeeWithMinSalary == julia, "Employ with minimum salary din't match")
  }

  def findEmployeesWithMissingSalaryInformation(records: DataFrame): Unit = {
    val numberOfEmployeesWithMissingSalary = records.where(col("salary").isNull).count()

    assert(numberOfEmployeesWithMissingSalary == 0, "Number of employees with missing salary did not match")
  }
}

case class Employee(id: Int, name: String, salary: Int, department: String, gender: String)