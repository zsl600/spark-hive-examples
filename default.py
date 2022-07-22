from pyspark.sql import SparkSession


spark = SparkSession.builder\
  .config("spark.hadoop.javax.jdo.option.ConnectionURL","jdbc:postgresql://localhost:5432/metastore")\
  .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")\
  .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "<postgres_user>")\
  .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "<postgres_password>")\
  .config("spark.hadoop.hive.metastore.schema.verification", False)\
  .config("spark.hadoop.datanucleus.autoCreateSchema", True)\
  .config("spark.hadoop.datanucleus.fixedDatastore", False)\
  .config("spark.hadoop.datanucleus.schema.autoCreateTables", True)\
  .config("spark.sql.warehouse.dir", "s3a://<bucket-name>/warehouse/")\
  .config("spark.driver.userClassPathFirst", True)\
  .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')\
  .enableHiveSupport()\
  .master("local[*]").appName("testSpark").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")



df = spark.read.option("header","true").csv("data.csv")
df.registerTempTable("input")


spark.sql("DROP TABLE IF EXISTS dataset_hive")

spark.sql("CREATE TABLE IF NOT EXISTS dataset_hive AS SELECT * FROM input")

spark.sql("SELECT * FROM dataset_hive").show(10)

spark.sql("SELECT region, SUM(arv_count) FROM dataset_hive GROUP BY region").show(10)


#Not supported in default Spark
# spark.sql("UPDATE TABLE dataset SET INDICATOR0 = '1234'")


