from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession
from pyspark import SparkFiles, SQLContext

spark = SparkSession.builder \
    .config("spark.driver.extraJavaOptions", "-Xss1024m") \
    .config("spark.executor.extraJavaOptions", "-Xss1024m") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "9g") \
    .appName("saveData") \
    .getOrCreate()
sqlContext = SQLContext(spark)
spark.sparkContext.setLogLevel('WARN')
sc=spark.sparkContext
#spark.sparkContext.addPyFile('./resources/SmartReduction.zip')
#import org.apache.spark.mllib.feature as F
# Data must be cached in order to improve the performance
#mllib = __import__("org.apache.spark.mllib.feature")

#df = spark.read.option("inferSchema", True) \
#.option("header",False) \
#.csv('hdfs://atlas:9000/user/datasets/ecbdl14/ECBDL14.dat')

#df2 = spark.read.text("hdfs://atlas:9000/user/carsan/headers.txt", lineSep=",")

#columnsNew = [str(row.value) for row in df2.collect()]
#print(columnsNew)
jvm = sc._jvm
df = spark.read.option("inferSchema", True) \
.option("header",False).parquet('./resources/proteinas100.parquet')

def parse(l):
  l = float(l)
  return LabeledPoint(l)

rdd= df.rdd.map(lambda row: LabeledPoint(row['label'], row['features'].toArray()))
print(type(rdd))
fcnn_mr_model = jvm.org.apache.spark.mllib.feature.RMHC_MR(rdd, 0.15, 5, 3, 172134)
#FCNN_MR(trainingData, k)

#fcnn_mr = fcnn_mr_model.runPR()

#df=df.toDF(*columnsNew)
df.show(truncate=False)
#=""
#with open('hdfs://atlas:9000/user/datasets/ecbdl14/ECBDL14.header', 'r') as file:
 #   data = file.read()
#print(data)
#h = data.split(", ")
#h.append('class')
#oldColumns=df.columns

#dict=dict((oldColumns[i], h[i]) for i in len(oldColumns))
#df=df.withColumnsRenamed(dict)
#print(df.columns)



#df.show(truncate=False, n=1000)