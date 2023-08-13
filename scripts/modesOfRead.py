#######################
# Author: Kishore Kumar Dayalan
# Release Date: 6/4/2023
# This script is useful in understanding the 3 types of data read modes
# eg 1|kishore|26|B.E
#    2|kumar|28
######################
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os 
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .master('local')\
        .appName('WindowFn')\
            .getOrCreate()
print(spark)
schema=StructType([
    StructField("name",IntegerType(),False),
    StructField("no",StringType(),False),
    StructField("age",IntegerType(),False),
    StructField("Qualification",StringType(),False)
])
df=spark.read.option("mode","DROPMALFORMED").csv('D:\sample_data.txt',sep='~|',schema=schema,header=True)
df.show()
df1=spark.read.option("mode","PERMISSIVE").csv('D:\sample_data.txt',sep='~|',schema=schema,header=True)
df1.show()
try:
    df2=spark.read.option("mode","FAILFAST").csv('D:\sample_data.txt',sep='~|',schema=schema,header=True)
    df2.show()
except Exception as e:
    if 'FAILFAST' in str(e):
        print('bad records found')
    else:
        print('error')
spark.stop()