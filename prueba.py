from functools import reduce
from random import uniform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, rand, lit, when, max, udf, product
from pyspark.sql.types import DoubleType


def getFields(c): return df.schema[c].dataType.names

def calculate(df, columns):
    df = reduce(lambda df, idx: \
          df.withColumn(columns[idx], \
                        struct(*([col(columns[idx])[c].alias(c) for c in getFields(columns[idx])] + \
                                 [(col(columns[idx]+".weight")*col(columns[idx]+".value")).alias("res")]))),
                                    range(len(columns)), df)
    df=reduce(lambda df, idx: \
          df.withColumn("sum", col("sum")+col(columns[idx]+".res")), \
                                    range(len(columns)), df)
    return reduce(lambda df, idx: \
          df.withColumn("sumTotal", col("sumTotal")+col(columns[idx]+".value")), \
                                    range(len(columns)), df)


def indexerStringColumns(df, cols):
    return reduce(lambda df, idx: StringIndexer(inputCol=cols[idx], outputCol="index", stringOrderType='frequencyAsc') \
      .fit(df).transform(df) \
      .withColumn(cols[idx], struct(rand().alias("weight"), col("index").alias("value"), col(cols[idx]).alias("original"))) \
      .drop("index"), range(len(cols)), df)

@udf(returnType=DoubleType())
def randomNumber(n,m):
    return uniform(n, m)

def updateWeight2(df, columns):
    df=reduce(lambda df, idx:df.withColumn(columns[idx], col(columns[idx]).withField("max", \
    when(col(columns[idx]+".res")>lit(corte), col(columns[idx]+".weight")).otherwise(col(columns[idx]+".max")))), \
    range(len(columns)), df)

    df=reduce(lambda df, idx: df.withColumn(columns[idx], col(columns[idx]).withField("min", \
     when(col(columns[idx] + ".res") < lit(corte), col(columns[idx] + ".weight")).otherwise(col(columns[idx] + ".min")))), \
     range(len(columns)), df)

    df=reduce(lambda df, idx: df.withColumn(columns[idx], col(columns[idx]).withField("weight", \
        when(col(columns[idx]+".res") < lit(corte), \
        randomNumber(col(columns[idx]+".min"), col(columns[idx]+".max"))) \
        .otherwise(when(col(columns[idx]+".res") > lit(corte), \
        randomNumber(col(columns[idx] + ".min"), col(columns[idx] + ".max")))))), \
        range(len(columns)), df)

    return reduce(lambda df, idx: df.withColumn(columns[idx], col(columns[idx]) \
            .withField("res",col(columns[idx] + ".weight") * col(columns[idx] + ".value"))), \
              range(len(columns)), df)

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .config("spark.driver.memory", "3g") \
      .appName("processingData") \
      .getOrCreate() 

df = spark.read \
.option("inferSchema" , True) \
.option("sep", "|") \
.option("header",True).csv("./resources/mydata.csv")
df.persist()
fieldsNotString = [ field.name for field in df.schema.fields if((field.jsonValue()["type"] != "string") and (field.name!="class"))]

df=reduce(lambda df, idx: \
          df.withColumn(fieldsNotString[idx], \
                        struct(rand().alias("weight"), col(fieldsNotString[idx]).alias("value"))), range(len(fieldsNotString)), df)


from pyspark.ml.feature import StringIndexer

cols=[ field.name for field in df.schema.fields if((field.jsonValue()["type"] == "string") and (field.name!="class"))]
print(cols)
df = indexerStringColumns(df, cols)

columns=[c for c in df.columns if(c!="class")]

df=df.withColumn("sum", lit(0))
df=df.withColumn("sumTotal", lit(0))
df=calculate(df, columns)

df=df.withColumn("total", col("sum")*col("class"))
df.persist()
max=df.filter("total>0").select(max(col("sumTotal")).alias("MAX")).limit(1).collect()[0].MAX
print(max)
corte=max/len(columns)
print(corte)
print(columns)

df.show(truncate=False)
x=0
df=reduce(lambda dfAux, idx:
          dfAux.withColumn(columns[idx], col(columns[idx]) \
           .withField("max", lit(1))), range(len(columns)), df)
df=reduce(lambda dfAux, idx:
          dfAux.withColumn(columns[idx], col(columns[idx]) \
           .withField("min", lit(0))), range(len(columns)), df)
df.persist()
dfFinal=reduce(lambda dfAux2, idx: updateWeight2(dfAux2, columns), range(1000), df)
df.unpersist()
dfFinal.persist()
dfSumaFinal=reduce(lambda dfAux, idx: \
    dfAux.withColumn("sum", col("sum")+col(columns[idx]+".res")), range(len(columns)), dfFinal)
dfFinal.unpersist()
dfSumaFinal.persist()
dfSumaFinal.select("sum").show(truncate=False)

prod=[dfSumaFinal.select(product(col(i+".weight")).alias("prod")).limit(1).collect()[0].prod for i in columns]
dfSumaFinal.unpersist()
print(prod)