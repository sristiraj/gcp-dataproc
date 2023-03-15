from pyspark.sql import SparkSession
from pyspark.streaming import DStream

spark = SparkSession.builder.appName("sample").master("local[*]").getOrCreate()

projectId = "data-lake-project-379413"
location = "us-east1-b"
subscription = "datalake-consumer"
df = spark.readStream.format("pubsublite").option("pubsublite.subscription",f"projects/{projectId}/locations/{location}/subscriptions/{subscription}").load()
df.writeStream.format("parquet").outputMode("append").trigger(processingTime="1 second").\
        option("path","/home/wicked/Downloads/gcp-pubsub/liteoutput/").\
        option("checkpointLocation","/home/wicked/Downloads/gcp-pubsub/liteoutput/_checkpoint").\
        start().awaitTermination()