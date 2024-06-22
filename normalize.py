from pyspark import SQLContext
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession
from functools import reduce

from pyspark.sql.functions import col, lit, when

def indexerStringColumns(df, cols):
    stringIndexer = StringIndexer()
    stringIndexer.setInputCols(cols)
    stringIndexer.setOutputCols([c+"_indexed" for c in cols])
    dictColumns = dict((c, col(c+"_indexed")) for c in cols)
    return stringIndexer.fit(df).transform(df).withColumns(dictColumns).drop(*[c+"_indexed" for c in cols])

# Create SparkSession
spark = SparkSession.builder \
    .config("spark.driver.extraJavaOptions", "-Xss1024m") \
    .config("spark.executor.extraJavaOptions", "-Xss1024m") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "9g") \
    .appName("saveData") \
      .getOrCreate()

sqlContext = SQLContext(spark)
spark.sparkContext.setLogLevel('WARN')

df2 = spark.read.text("hdfs://atlas:9000/user/carsan/headers.txt", lineSep=",")

columnsNew = [str(row.value) for row in df2.collect()]
print(columnsNew)

print("Leer fichero")
df = spark.read.option("inferSchema", True) \
.option("header",False) \
.csv('hdfs://atlas:9000/user/datasets/ecbdl14/ECBDL14.dat')
df = df.toDF(*columnsNew)
df.cache()
columns = [c for c in df.columns if(c!="class")]

colsString = [field.name for field in df.schema.fields if((field.jsonValue()["type"] == "string") and (field.name!="class"))]

print(colsString)
print("Indexer string sobre las columnas")
dfIndexed=indexerStringColumns(df,colsString)
dfIndexed.cache()

print("Calcular maximos y minimos")
listMax=["max(`"+c+"`) as `"+c+"`" for c in columns]
listMin=["min(`"+c+"`) as `"+c+"`" for c in columns]
max = dfIndexed.selectExpr(*listMax).first().asDict()
min = dfIndexed.selectExpr(*listMin).first().asDict()

print("Normalizar columnas")
dictCols = dict((c, col(c)-lit(min[c])/lit(max[c])-lit(min[c])) for c in columns)
dfNormalize = dfIndexed.withColumns(dictCols) \
    .withColumn("class", when(col("class")==lit("negative"), lit(0)).otherwise(lit(1)))

print("Guardar dataset normalizado")
dfNormalize.write.parquet("hdfs://atlas:9000/user/carsan/proteinasNormalized.parquet")


