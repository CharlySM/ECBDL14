import operator
from random import uniform

from pyspark import SQLContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, rand, struct, lit, when, udf, product, max, avg, expr, log, exp, ln, \
    sum_distinct
from functools import reduce
from pyspark.sql.types import DoubleType, StringType
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


def updateWeight3(df, transform):
    return df.withColumns(dictMaxMinW)



# Create SparkSession

spark = SparkSession.builder \
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
#dfWeights.show(10, truncate=False)
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
max=df2.filter("total>0").select(max(col("sumTotal")).alias("MAX")).first()[0]

corte=max/len(columns)
print(corte)
print("Calcular maximo y minimo")
cont=0
df2=df2.select(*[columns])
dictMaxMin=dict((c, col(c).withField("max", lit(1)).withField("min", lit(0))) for c in columns)
df2=df2.withColumns(dictMaxMin).cache()
#df2.cache()

dictMaxMinW = dict((c, col(c).withField("max", when(col(c + ".res") > lit(corte), col(c + ".weight")).otherwise(
    col(c + ".max"))).withField("min", when(col(c + ".res") < lit(corte), col(c + ".weight"))
    .otherwise(col(c + ".min"))).withField("weight", randomNumber(col(c + ".min"), col(c + ".max")))
    .withField("res", col(c + ".weight") * col(c + ".value"))) for c in columns)

print("Actualizar pesos de las columnas")

import time
start_time = time.time()
df2Updated=reduce(lambda df2Aux, transform: df2Aux.transform(updateWeight3, transform), range(10), df2)
print("termina actualiza pesos")
print("--- %s seconds ---" % ((time.time() - start_time)/60))
df2Sum=df2Updated.cache()
#df2Sum=df2Updated.withColumn("sum", lit(0)).cache()

print("Calcular suma con pesos finales")
listSel=[struct(col(f"{c}.weight"), col(f"{c}.value")).alias(c) for c in columns]
#dfSumaFinal = df2Sum.withColumn('sum', sum(df2Sum[c+".res"] for c in columns)).select(*[listSel]).cache()
dfSumaFinal = df2Sum.select(*[listSel]).limit(2).cache()
#dfSumaFinal.printSchema()
#dfSumaFinal.filter(col("separation.weight").isNull()).show(10, truncate=False)
print("Calcular productos")
#print(dfSumaFinal.count())
epsilon = 1e-9
#windowSpec = Window.orderBy(expr("monotonically_increasing_id()"))
dictSum=dict((c, expr(f"sum('{c}.weight'+{epsilon})")) for c in columns)
#dictSum=dict((c, expr(f"EXP(SUM(LOG('{c}.weight'+{epsilon})) over order by monotonically_increasing_id())")) for c in columns)
#dictSum=dict((c, product(col(f"{c}.wieght"))) for c in columns)


prod=dfSumaFinal.withColumns(dictSum)
prod.show(10, truncate=False)
#dictProd=dict((c, product(col(c+".weight"))) for c in columns)
#strProd=[f"sum('{c}.weight') as `"+c+"`" for c in columns]
#strProd=[sum(col(f"{c}.weight")).alias(c) for c in columns]
#prod=prod.first().asDict()
#print(prod)
'''
print(strProd)
prod=dfSumaFinal.selectExpr(*[strProd])
#prod=dfSumaFinal.select(*[strProd]) 

#dfSumaFinal.show(truncate=False)
from heapq import nlargest
prod.show(12, truncate=False)
prod=prod.first().asDict()
print(prod)
five_largest = dict(sorted(prod.items(), key= operator.itemgetter(1), reverse=True))

five_largest={key:value for key,value in list(five_largest.items())[0:90]}
print(five_largest)

colM=lit(1)
for i in columns:
    colM=colM*col(i+".weight")

dfProd = dfSumaFinal.withColumn('valueRow', colM)

minT=dfProd.selectExpr("min(`valueRow`) as MIN").limit(1).collect()[0].MIN
avg=dfProd.selectExpr("avg(`valueRow`) as AVG").limit(1).collect()[0].AVG

keys=[col(i+".value").alias(i) for i in list(five_largest.keys())]

dfFinalTotal=dfProd.filter(col("valueRow")>=lit(avg-minT)).select(*[keys]).cache()
#dfFinalTotal.show(truncate=False)
print(dfFinalTotal.count())
#dfFinalTotal.printSchema()
print("Escribir dataset")
#dfFinalTotal.write.parquet('hdfs://atlas:9000/user/carsan/proteinasPesos.parquet')
'''