from pyspark.sql import SparkSession


spark = SparkSession \
  .builder \
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
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .enableHiveSupport()\
  .master("local[*]").appName("testSpark").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header","true").csv("data.csv")
df.registerTempTable("input")

spark.sql("DROP TABLE IF EXISTS default.dataset_delta")

spark.sql("CREATE OR REPLACE TABLE default.dataset_delta USING DELTA AS SELECT * FROM input")

spark.sql("SELECT * FROM default.dataset_delta LIMIT 10").show()

spark.sql("UPDATE default.dataset_delta SET period = 'NA' ")

spark.sql("SELECT * FROM default.dataset_delta LIMIT 10").show()

spark.sql("SELECT region, SUM(arv_count) FROM default.dataset_delta GROUP BY region LIMIT 10").show()

# spark.sql("DESC FORMATTED dataset_delta").show()
