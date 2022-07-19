from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import from_json, col
from os import getenv

KAFKA_HOST=getenv('KAFKA_HOST')
KAFKA_PORT=getenv('KAFKA_PORT')
TOPIC=getenv('TOPIC')
CASS_TABLE=getenv('CASS_TABLE')
CASS_KEYSPACE=getenv('CASS_KEYSPACE')
CASS_HOST=getenv('CASS_HOST')
CASS_PORT=getenv('CASS_PORT')

schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("uuid", StringType(), False),
                StructField("number", IntegerType(), False)
            ])

spark = SparkSession \
    .builder \
    .appName("SparkStructuredStreaming") \
    .config("spark.cassandra.connection.host", CASS_HOST)\
    .config("spark.cassandra.connection.port", CASS_PORT)\
    .config("spark.cassandra.auth.username", "cassandra")\
    .config("spark.cassandra.auth.password", "cassandra")\
    .config("spark.driver.host", "localhost")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", f'{KAFKA_HOST}:{KAFKA_PORT}') \
  .option("subscribe", TOPIC) \
  .option("delimeter", ",") \
  .option("startingOffsets", "earliest") \
  .load()
df.printSchema()
df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
df1.printSchema()


def writeToCassandra(writeDF, _):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=CASS_TABLE, keyspace=CASS_KEYSPACE)\
        .save()


df1.writeStream \
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .start()\
    .awaitTermination()
df1.show()
