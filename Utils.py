from pyspark.sql.functions import struct, rand, col, lit


def balancearDF(df):
    dfN = df.filter("class=0")
    dfC1 = df.filter("class=1")

    nP = dfC1.count()
    nN = dfN.count()
    ratio = (nP / nN)
    dfN = dfN.sample(False, ratio)
    return dfC1.unionAll(dfN)

def getColumns(df):
    return [c for c in df.columns if(c!="class")]

def initializingWeights(df,columns):
    dictWeights = dict((c, struct(rand().alias("weight"), col(c).alias("value"))) for c in columns)

    return df.withColumns(dictWeights)

def initializingSumas(df):
    dfSum = df.withColumn("sum", lit(0))
    return dfSum.withColumn("sumTotal", lit(0))