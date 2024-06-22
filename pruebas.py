from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, lit

def getFields(c):
    if df.schema[c].name=='struct':
       return df.schema[c].fieldNames()
    else :
        return ""

spark = SparkSession.builder \
    .config("spark.driver.extraJavaOptions", "-Xss512m") \
    .config("spark.executor.extraJavaOptions", "-Xss512m") \
    .getOrCreate()

df = spark.read \
.option("inferSchema" , True) \
.option("sep", "|") \
.option("header",True).csv("./resources/mydata.csv") \
.withColumn("mamifero", struct(col("mamifero").alias("campo1"), lit(2).alias("campo2"))) \
.select("class", "mamifero")

df=df.select([getFields("mamifero")])

df.show(truncate=False)