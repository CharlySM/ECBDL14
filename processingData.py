import operator
from random import uniform

from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rand, struct, lit, when, udf, product, max
from functools import reduce
from pyspark.sql.types import DoubleType
import Utils

def calculate(df, columns):
    d=dict((c, struct(col(c)["weight"].alias("weight"), col(c)["value"].alias("value"),
    (col(c+".weight")*col(c+".value")).alias("res"))) for c in columns)

    df = df.withColumns(d)

    df = df.withColumn('sum', sum(df[col+".res"] for col in columns))

    return df.withColumn('sumTotal', sum(df[col+".value"] for col in columns))


@udf(returnType=DoubleType())
def randomNumber(n,m):
    return uniform(n, m)

#def    generate_transformations(n):
 #   transformations = []
 #   for i in range(n):
def updateWeight2(df, columns):
     dictMaxMinW = dict((c, col(c).withField("max",
     when(col(c + ".res") > lit(corte), col(c + ".weight")).otherwise(
     col(c + ".max"))).withField("min",when(col(c + ".res") < lit(corte),
     col(c + ".weight")).otherwise(col(c + ".min")))) for c in columns)
    # print("actualiza max y min")
     dfMinMax = df.withColumns(dictMaxMinW)
     dictWeightsW = dict(
        (c, col(c).withField("weight", randomNumber(col(c + ".min"), col(c + ".max")))) for c in columns)
    # print("Actualiza los pesos")
     dfWeights = dfMinMax.withColumns(dictWeightsW)
     dictRes = dict((c, col(c).withField("res", col(c + ".weight") * col(c + ".value"))) for c in columns)
    # print("calcula res")
     return dfWeights.withColumns(dictRes)



def updateWeight3(df, transform):
    return df.withColumns(dictMaxMinW)



# Create SparkSession

'''.config("spark.driver.extraJavaOptions", "-Xss1024m") \
    .config("spark.executor.extraJavaOptions", "-Xss1024m") \
    .config("spark.executor.memory", "5g") \
    .config("spark.driver.memory", "5g") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "9g")  \ 
    .master("spark://atlas:7077") \
    .
'''

spark = SparkSession.builder \
    .config("spark.driver.extraJavaOptions", "-Xss1024m") \
    .config("spark.executor.extraJavaOptions", "-Xss1024m") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "9g")  \
    .appName("Processing Data") \
    .getOrCreate()

sqlContext = SQLContext(spark)
spark.sparkContext.setLogLevel('WARN')
print("Leer fichero")
df = spark.read.parquet('hdfs://atlas:9000/user/carsan/proteinasNormalized.parquet')
#df = spark.read.parquet('./resources/proteinasNormalized.parquet')
print("Balancear dataset")

dfTotal = Utils.balancearDF(df)
df.unpersist()

columns=Utils.getColumns(dfTotal)
print("Actualizar pesos")

dfWeights=Utils.initializingWeights(dfTotal, columns).cache()
dfTotal.unpersist()

print("Calcular sumas")
dfSumTotal=Utils.initializingSumas(dfWeights)
dfWeights.unpersist()
dfCalculated=calculate(dfSumTotal, columns)
dfSumTotal.unpersist()
print("Calcular Total")
df2=dfCalculated.withColumn("total", col("sumTotal")*col("class")).cache()
dfCalculated.unpersist()
print("Calcular maximo")
max=df2.filter("total>0").select(max(col("sumTotal")).alias("MAX")).limit(1).collect()[0].MAX

corte=max/len(columns)
print(corte)
print("Calcular maximo y minimo")
cont=0
dictMaxMin=dict((c, col(c).withField("max", lit(1)).withField("min", lit(0))) for c in columns)
df2=df2.withColumns(dictMaxMin).cache()
#df2.cache()
dictMaxMinW = dict((c, col(c).withField("max", when(col(c + ".res") > lit(corte), col(c + ".weight")).otherwise(
    col(c + ".max"))).withField("min", when(col(c + ".res") < lit(corte), col(c + ".weight"))
    .otherwise(col(c + ".min"))).withField("weight", randomNumber(col(c + ".min"), col(c + ".max")))
    .withField("res", col(c + ".weight") * col(c + ".value"))) for c in columns)

print("Actualizar pesos")
#transformations = generate_transformations(10000)
import time
start_time = time.time()
df2Updated=reduce(lambda df2Aux, transform: df2Aux.transform(updateWeight3, transform), range(1000), df2)
print("termina actualiza pesos")
print("--- %s seconds ---" % ((time.time() - start_time)/60))
df2Sum=df2Updated.withColumn("sum", lit(0)).cache()
print("Calcular suma con pesos finales")
dfSumaFinal = df2Sum.withColumn('sum', sum(df2Sum[c+".res"] for c in columns)).cache()
#dfSumaFinal.cache()
print("Calcular productos")
prod=dfSumaFinal.select(*[product(dfSumaFinal[c+".weight"]).alias(c) for c in columns]).first().asDict()

from heapq import nlargest

five_largest = dict(sorted(prod.items(), key=lambda item: item[1]))

five_largest={key:value for key,value in list(five_largest.items())[0:90]}

print(five_largest)

print("Escribir dataset")
dfSumaFinal.write.parquet('hdfs://atlas:9000/user/carsan/proteinasPesos.parquet')