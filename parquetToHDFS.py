from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .master("local") \
      .appName("processingData") \
      .getOrCreate()

df = spark.read.parquet('resources/proteinasNormalized.parquet')
df.show(truncate=False)
#df.write.parquet('"hdfs://proteinasNormalized.parquet')
