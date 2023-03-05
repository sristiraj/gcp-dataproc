from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

conf = SparkConf()
conf.setAppName("sample")
conf.set("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
# sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.csv("gs://demo-datalake-infoservice/bronze/transborderrawdata1")
# df.write.format("delta").mode("overwrite").option("path","gs://demo-datalake-infoservice/bronze/transborderrawdata2").saveAsTable("default.sample")

from delta.tables import *

delta_table = DeltaTable.forPath(spark, "gs://demo-datalake-infoservice/bronze/transborderrawdata2")


# This fails
delta_table.generate("symlink_format_manifest")