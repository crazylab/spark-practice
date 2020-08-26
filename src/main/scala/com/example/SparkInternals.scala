package com.example

import com.example.analysis.Employ
import org.apache.spark.sql.{SparkSession, DataFrame, Row, Column}
import org.apache.spark.sql.functions._

object SparkInternals {
  // Broadcast variable
  //  Accumulator

  val spark = SparkSession.builder()
    .appName("Spark Internals")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val employees = List(
      Employ(id = 1, name = "John", salary = 5000, department = "sales", gender = "M"),
      Employ(id = 2, name = "Jacob", salary = 10000, department = "HR", gender = "M"),
      Employ(id = 3, name = "Julia", salary = 1000, department = "sales", gender = "F"),
      Employ(id = 4, name = "Jackson", salary = 25000, department = "marketing", gender = "M"),
      Employ(id = 5, name = "Anna", salary = 8000, department = "HR", gender = "F"),
      Employ(id = 6, name = "Linda", salary = 12000, department = "HR", gender = "F"),
      Employ(id = 7, name = "Devid", salary = 5000, department = "marketing", gender = "M"),
      Employ(id = 8, name = "Peter", salary = 4000, department = "sales", gender = "M"),
      Employ(id = 9, name = "Bob", salary = 3000, department = "marketing", gender = "M"),
      Employ(id = 10, name = "Robin", salary = 7000, department = "marketing", gender = "F")
    ).toDF
      .repartition(3)
      .cache()

    makeNamesCapital(employees)
    convertGenderToBinary(employees)
    convertToNamePipeDept(employees)
    convertRowToCSVUsingUDF(employees)

    countNumberOfRecordsUsingAccumulator(employees)
    replaceNameWithNickNameUsingBroadcast(employees)
  }

  def makeNamesCapital(records: DataFrame): Unit = {
    val toUpper: String => String = _.toUpperCase()
    val toUpperUDF = udf(toUpper)
    import spark.implicits._
    val capitalNames = records.select(toUpperUDF(col("name"))).as[String].collect().sorted

    val expected = Array("JOHN", "JACOB", "JULIA", "JACKSON", "ANNA", "LINDA", "DEVID", "PETER", "BOB", "ROBIN").sorted
    assert(expected sameElements capitalNames, "Converting name to capital didn't work")
  }

  def convertGenderToBinary(records: DataFrame): Unit = {
    val gendersInBinary = records.select(when(col("gender") === "M", 0)
      .otherwise(1)
      .as("gender"))

    import spark.implicits._
    val expected = List(0, 0, 1, 0, 1, 1, 0, 0, 0, 1).toDF("gender")

    assert(gendersInBinary.exceptAll(expected).count() == 0, "Gender to binary conversion didn't match")
  }

  def convertToNamePipeDept(records: DataFrame): Unit = {
    def toNamePipeDept(name: String, dept: String): String = s"$name | $dept"

    val namePipeDeptUDF = udf(toNamePipeDept _)
    val nameAndDept = records.select(namePipeDeptUDF(col("name"), col("department")).as("name | dept"))

    import spark.implicits._
    val expected = List(
      "Jackson | marketing",
      "Linda | HR",
      "Devid | marketing",
      "Bob | marketing",
      "John | sales",
      "Jacob | HR",
      "Anna | HR",
      "Julia | sales",
      "Peter | sales",
      "Robin | marketing"
    ).toDF("name | dept")
    assert(nameAndDept.exceptAll(expected).count() == 0, "Name dept string creation didn't work")
  }

  def convertRowToCSVUsingUDF(records: DataFrame): Unit = {
    def toCsv(row: Row): String = row.mkString(",")

    val csvUDF = udf(toCsv _)

    val columns = records.columns.map(col)
    val csvRecords = records.select(csvUDF(struct(columns: _*)).as("csvRecord"))

    import spark.implicits._
    val employees = List(
      "1,John,5000,sales,M",
      "2,Jacob,10000,HR,M",
      "3,Julia,1000,sales,F",
      "4,Jackson,25000,marketing,M",
      "5,Anna,8000,HR,F",
      "6,Linda,12000,HR,F",
      "7,Devid,5000,marketing,M",
      "8,Peter,4000,sales,M",
      "9,Bob,3000,marketing,M",
      "10,Robin,7000,marketing,F"
    ).toDF("csvRecord")
    assert(employees.exceptAll(csvRecords).count() == 0, "CSV records didn't match")
  }

  def replaceNameWithNickNameUsingBroadcast(records: DataFrame): Unit = {
    val nickNames = Map(
      "John" -> "J",
      "Jacob" -> "Jay",
      "Julia" -> "Jul",
      "Jackson" -> "Jack",
      "Anna" -> "Ann",
      "Linda" -> "Lin",
      "Devid" -> "Dev",
      "Peter" -> "Pete",
      "Robin" -> "Rob"
    )

    val nickNameBroadcast = spark.sparkContext.broadcast[Map[String, String]](nickNames)
    val nickNameUDF = udf((name: String) => nickNameBroadcast.value.getOrElse(name, "No Nick Name"))
    val employesWithNickName = records.withColumn("name", nickNameUDF(col("name")))

    import spark.implicits._
    val expected = List(
      Employ(id = 1, name = "J", salary = 5000, department = "sales", gender = "M"),
      Employ(id = 2, name = "Jay", salary = 10000, department = "HR", gender = "M"),
      Employ(id = 3, name = "Jul", salary = 1000, department = "sales", gender = "F"),
      Employ(id = 4, name = "Jack", salary = 25000, department = "marketing", gender = "M"),
      Employ(id = 5, name = "Ann", salary = 8000, department = "HR", gender = "F"),
      Employ(id = 6, name = "Lin", salary = 12000, department = "HR", gender = "F"),
      Employ(id = 7, name = "Dev", salary = 5000, department = "marketing", gender = "M"),
      Employ(id = 8, name = "Pete", salary = 4000, department = "sales", gender = "M"),
      Employ(id = 9, name = "No Nick Name", salary = 3000, department = "marketing", gender = "M"),
      Employ(id = 10, name = "Rob", salary = 7000, department = "marketing", gender = "F")
    ).toDF

    assert(employesWithNickName.exceptAll(expected).count() == 0, "Employ with nick name didn't match")
  }

  def countNumberOfRecordsUsingAccumulator(records: DataFrame): Unit = {
    val countAccumulator = spark.sparkContext.longAccumulator("Count Accumulator")
    records.foreachPartition((partition: Iterator[Row]) => {
      countAccumulator.add(partition.length)
    })

    assert(countAccumulator.value == records.count(), "Record count did not match")
  }
}
