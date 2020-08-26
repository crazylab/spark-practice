package com.example.io

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}

/* Notes:
Search for # to find the important pieces to note
*/
object ReadWriteOperations {
  val spark = SparkSession.builder()
    .appName("File Writer")
    .master("local[*]")
    .getOrCreate();

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val employees = List(
      Employ(id = 1, name = "John", salary = 5000, departmentID = "sales"),
      Employ(id = 2, name = "Jacob", salary = 10000, departmentID = "HR"),
      Employ(id = 3, name = "Julia", salary = 1000, departmentID = "sales"),
      Employ(id = 4, name = "Jackson", salary = 25000, departmentID = "marketing"),
      Employ(id = 5, name = "Anna", salary = 8000, departmentID = "HR"),
      Employ(id = 6, name = "Linda", salary = 12000, departmentID = "HR"),
      Employ(id = 7, name = "Devid", salary = 5000, departmentID = "marketing"),
      Employ(id = 8, name = "Peter", salary = 4000, departmentID = "sales"),
      Employ(id = 9, name = "Bob", salary = 3000, departmentID = "marketing"),
      Employ(id = 10, name = "Robin", salary = 7000, departmentID = "marketing")
    ).toDF.cache()

    //    Save the employees data in HDFS in different file format
    //    --------------------------------------------------------
    //    Save as plaintext
    //    employees.write.text("src/main/resources/data/employees.txt") // Text data doesn't support Int data type

    //    -------Save Employees as AVRO file format-------
    readWriteAVROData(employees, "src/main/resources/data/employees.avro")

    //    -------Save Employees as JSON file format-------
    readWriteJSONData(employees, "src/main/resources/data/employees.json")

    //    -------Save Employees as Parquet file format-------
    readWriteParquetData(employees, "src/main/resources/data/employees.parquet")

    //    -------Save Employees as CSV file format-------
    readWriteCSVData(employees, "src/main/resources/data/employees.csv")

    //    -------Save Employees as Pipe Separated file format-------
    readWritePipeSeparatedData(employees, "src/main/resources/data/employees.pipe")

    //    -------Save Employees data partitioned by department-------
    readWritePartitionedData(employees, List("departmentID"), "src/main/resources/data/employees_with_part.csv")

    //    -------Read csv data without header using custom schema-------
    readCSVWithoutSchema(employees, "src/main/resources/data/employees_without_header.csv")

    //    --------------------------------------------------------
    //    Save in hive metastore
  }

  def readWriteAVROData(data: DataFrame, path: String): Unit = {
    println("Writing AVRO file")

    data.write
      .format("avro") // #Need to import org.apache.spark:spark-avro_2.12:2.4.0 to use this option
      .mode(SaveMode.Overwrite) // #Options available: append, overwrite, ignore, error, errorifexists. More details: https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes
      .save(path) // #Should be using save() while using write.format()

    val avroData = spark.read
      .format("avro")
      .load(path)

    assert(avroData.exceptAll(data).count() == 0, "AVRO data count did not match")
  }

  def readWriteJSONData(data: DataFrame, path: String): Unit = {
    println("Writing JSON file")
    data.write
      .mode("overwrite")
      .json(path)

    val jsonData = spark.read
      .schema(data.schema) // #To provide custom schema
      .json(path)

    assert(jsonData.exceptAll(data).count() == 0, "JSON data count did not match")
  }

  def readWriteParquetData(data: DataFrame, path: String): Unit = {
    println("Writing Parquet file")
    data.write
      .mode("overwrite")
      .parquet(path)

    val parquetData = spark.read
      .parquet(path)

    assert(parquetData.exceptAll(data).count() == 0, "Parquet data count did not match")
  }

  def readWriteCSVData(data: DataFrame, path: String): Unit = {
    println("Writing CSV file")
    data.write
      .mode("overwrite")
      .option("header", true)
      .csv(path)

    val csvData = spark.read
      .option("header", true)
      .csv(path)

    assert(csvData.exceptAll(data).count() == 0, "CSV data count did not match")
  }

  def readWritePipeSeparatedData(data: DataFrame, path: String): Unit = {
    println("Writing pipe separated file")
    data.write
      .mode("overwrite")
      .option("header", true) // #Remove option("header", true) to write without header
      //      .option("delimiter", "|")
      .option("sep", "|") // #Either "delimiter" or "sep" can be specified to provide custom delimiter
      .csv(path)

    val pipeSeparatedData = spark.read
      .option("header", true)
      .option("delimiter", "|")
      .csv(path)

    assert(pipeSeparatedData.exceptAll(data).count() == 0, "Pipe Separated data count did not match")
  }

  def readWritePartitionedData(data: DataFrame, partitionBy: List[String], path: String): Unit = {
    print(s"Writing data with $partitionBy partition")
    data.write
      .partitionBy(partitionBy: _*) // #Partition by columns provided
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("maxRecordsPerFile", 1) // #Set max number of records per partition file
      .option("quote", "") // #Converts quotes in the data to empty char
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .csv(path)

    val partitionedData = spark.read
      .option("header", true)
      .option("inferSchema", true) // #Infer the data of the schema
      .csv(path)

    assert(partitionedData.exceptAll(data).count() == 0, "Partitioned data count did not match")
  }

  def readCSVWithoutSchema(data: DataFrame, path: String): Unit = {
    print("Writing data without header")
    data.write
      .mode(SaveMode.Overwrite)
      .csv(path)

    val employeeSchema: StructType = StructType(
      StructField("ID", IntegerType) ::
        StructField("Name", StringType) ::
        StructField("Salary", IntegerType) ::
        StructField("DepartmentId", StringType) ::
        Nil
    )

    val employData = spark.read
      .schema(employeeSchema) // #Either use schema() or read as dataset by providing case class while inferring schema
      .csv(path)

    assert(employData.exceptAll(data).count() == 0, "Data without header count did not match")
    assert(employData.schema == employeeSchema, "Schema did not match")
  }

  //  TODO: Read/Write file with custom empty value
  /*
  Set option("emptyValue", "") #
   */
}

case class Employ(id: Int, name: String, salary: Int, departmentID: String)