**Spark Practice**
--
This project contains spark practice material


---
Create a fat jar by running the following command from the project root directory
```bash
sbt clean compile assembly
```
The above command will create a jar file under `./target/scala-{x.x}/{project-name}-assembly-{version}-SNAPSHOT.jar`.

The job can be submitted using the following command
```bash
spark-submit --class com.example.Main --master local[*] ./target/scala-2.12/scala-spark-project-assembly-0.1.0-SNAPSHOT.jar
```

---
Spark Submit Command with Configuration Parameters
```shell script
spark-submit --class com.example.Main \
  --master local[*]  exam.jar \
  --packages org.apache.spark:spark-avro_2.12:2.4.0
  --num-executors 5
  --executor-memory 1000M
  --driver-memory 2G
```
---
Print list of HDFS dir recursively:
```shell script
hdfs dfs -ls -R <hdfs_path>
```
---
Build a jar out of a scala file:
```shell script
scala -save file.scala 
```

