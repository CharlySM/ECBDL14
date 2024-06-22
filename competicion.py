# Import SparkSession
from pyspark.sql import SparkSession
from functools import reduce


# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("saveData") \
      .getOrCreate() 

# Create RDD from external Data source
df = spark.read \
.option("inferSchema" , True) \
.option("header",False).csv("../mydata100.data")
#df.printSchema()

data=""
with open('../headers.txt', 'r') as file:
    data = file.read()
#print(data)
h = data.split(", ")
h.append('class')
#print(h)
oldColumns=df.columns
#print(h)
df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], h[idx]), range(len(oldColumns)), df)
df.write.parquet("./resources/proteinas100.parquet")


#print(h)



#df.show(truncate=False)
#df.printSchema()
