# Sample code of Spark with Hive Metastore and Datalake(CSV) and Lakehouse(Delta Lake)

Code sample for Spark + Hive Metastore + CSV + Delta Lake

## Getting Started

### Dependencies

* Python3
* PIP

### Installing pre-requisites

```
pip install pyspark==3.2.1
pip install delta-spark==1.2.1
wget https://jdbc.postgresql.org/download/postgresql-42.4.0.jar -O aws-java-sdk-bundle.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.3/hadoop-aws-3.2.3.jar -O aws-java-sdk-bundle.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.264/aws-java-sdk-bundle-1.12.264.jar -O aws-java-sdk-bundle.jar
```

### Running the code
* For Spark + Hive metastore
```
spark-submit --master local --jars "postgresql.jar,hadoop-aws.jar,aws-java-sdk-bundle.jar" --driver-class-path "postgresql.jar"  default.py
```

* For Spark + Hive + Delta Lake
spark-submit --master local --jars  "postgresql.jar,hadoop-aws.jar,aws-java-sdk-bundle.jar"  --driver-class-path "postgresql.jar"  --packages io.delta:delta-core_2.12:1.2.1 deltalake.py
```
