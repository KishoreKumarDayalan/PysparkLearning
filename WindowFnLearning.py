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
df=spark.read.csv('D:\PySpark-Tutorial-main\emp.csv',header=True,inferSchema=True)
df.createOrReplaceTempView('emp')
df1=df.withColumn('Sum',sum('salary').over(Window.partitionBy('dept')))\
    .withColumn('row_num',row_number().over(Window.partitionBy('dept').orderBy('year')))
df1.filter(df1.row_num==1).show()
df2=spark.sql("select *,sum(salary)over(partition by dept) as sum,row_number()over(partition by dept order by year) as row_num from emp;")
df2.filter(df2.row_num==1).show()
spark.stop()