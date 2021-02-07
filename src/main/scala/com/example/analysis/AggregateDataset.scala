package com.example.analysis

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object AggregateDataset {

  val spark = SparkSession.builder()
    .appName("Spark Aggregations")
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

    averageSalary(employees)
    averageSalaryPerDept(employees)
    employeesPerDept(employees)
    secondHighestEarningEmployee(employees)
    numberOfEmployeesPerDepartment(employees)

    wordCount()

  }

  def averageSalary(records: DataFrame): Unit = {
    val averageSalary = records
      .select(avg("salary").as("avgSalary"))
      .first()
      .getAs[Double]("avgSalary")

    assert(averageSalary == 8000, "Average salary didn't match")
  }

  def averageSalaryPerDept(records: DataFrame): Unit = {
    val averageSalaryPerDept = records.groupBy("department").avg("salary").as("average_salary")

    import spark.implicits._
    val expected = (("HR", 10000.0) :: ("marketing", 10000.0) :: ("sales", 3333.3333333333335) :: Nil)
      .toDF("department", "avg_salary")

    assert(averageSalaryPerDept.exceptAll(expected).count() == 0, "Average salary per department didn't match")
  }

  def employeesPerDept(records: DataFrame): Unit = {
    val employeesPerDept = records
      .groupBy("department")
      .agg(sort_array(collect_list("name")).as("employees"))

    import spark.implicits._
    val expected = (("HR", Array("Anna", "Jacob", "Linda"))
      :: ("marketing", Array("Bob", "Devid", "Jackson", "Robin"))
      :: ("sales", Array("John", "Julia", "Peter")) :: Nil)
      .toDF("department", "employees")

    assert(employeesPerDept.exceptAll(expected).count() == 0, "Employees per dept didn't match")
  }

  def secondHighestEarningEmployee(records: DataFrame): Unit = {
    import spark.implicits._
    val orderBySalary = Window.orderBy($"salary".desc)
    val employWithSecondHighestSalary = records
      .withColumn("rank", dense_rank().over(orderBySalary))
      .where($"rank" === 2)
      .drop("rank")
      .as[Employee]
      .first()

    val linda = Employee(id = 6, name = "Linda", salary = 12000, department = "HR", gender = "F")
    assert(linda == employWithSecondHighestSalary, "Employ with second highest salary didn't match")
  }

  def numberOfEmployeesPerDepartment(records: DataFrame): Unit = {
    val employeesPerDept = records.groupBy("department").count()

    import spark.implicits._
    val expected = (("HR", 3)
      :: ("marketing", 4)
      :: ("sales", 3) :: Nil)
      .toDF("department", "employ_count")

    assert(expected.exceptAll(employeesPerDept).count() == 0, "Number of employees per dept didn't match")
  }

  def wordCount(): Unit = {
    import spark.implicits._
    val text =
      """This text is for word count
        |This will count number of word""".stripMargin
    val textDF = text.split("\n").toList.toDF("text")

    val wordArray = split(col("text"), " ")
    val wordCount = textDF.select(explode(wordArray).as("words"))
      .groupBy(lower(col("words").as("word"))).count()


    val expected = (("will", 1)
      :: ("for", 1)
      :: ("word", 2)
      :: ("count", 2)
      :: ("is", 1)
      :: ("of", 1)
      :: ("text", 1)
      :: ("this", 2)
      :: ("number", 1)
      :: Nil)
      .toDF("word", "count")

    assert(expected.exceptAll(wordCount).count() == 0, "Word count didn't match")
  }
}
