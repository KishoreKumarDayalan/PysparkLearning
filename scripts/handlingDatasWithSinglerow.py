#######################
# Author: Kishore Kumar Dayalan
# Release Date: 6/4/2023
# This script is useful in spliting the datafile which has all data in single row with '|' delimitter
# eg 1|kishore|26|2|kumar|28
######################
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os 
import sys
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .master('local')\
        .appName('WindowFn')\
            .getOrCreate()
print(spark)
df=spark.read.csv('D:\sample_data.txt')
df.select("*",regexp_replace("_c0","(.*?\\|){3}","$0-")).show(truncate=0)
df=df.withColumn("_c0",regexp_replace("_c0","(.*?\\|){3}","$0-"))
df.show()
df=df.withColumn("_c0",explode(split("_c0","\|-")))
df.show()
########option 1######
df.rdd.map(lambda x: x[0].split("|")).toDF(['no','name','age']).show()

########option 2######
rdd2=df.rdd.collect()
series=list(map(lambda x: x[0].split("|"),rdd2))
print(series)
df1=spark.createDataFrame(series,['no','name','age'])
df1.show()
#####################
spark.stop()