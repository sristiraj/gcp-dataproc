from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
import base64
# from pyspark.streaming import Milliseconds

conf = SparkConf()
# conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
conf.set("spark.jars.packages","io.delta:delta-core_2.12:1.0.1")
conf.set("spark.jars","pubsublite-spark-sql-streaming-1.0.0-with-dependencies.jar")
spark = SparkSession.builder.config(conf=conf).master("local[*]").getOrCreate()
# ssc = StreamingContext(spark.sparkContext(), Milliseconds(100))

projectId = "data-lake-project-379413"
location = "us-east1-b"
subscription = "datalake-lite-sub"
with open("adc.json") as f:
    creds = bytes(f.read(),'utf-8')
creds64 = base64.b64encode(creds)
df = spark.readStream.format("pubsublite").option("gcp.credentials.key",creds64).option("pubsublite.subscription",f"projects/{projectId}/locations/{location}/subscriptions/{subscription}").load()

df.writeStream.format("console").outputMode("append").start().awaitTermination()
