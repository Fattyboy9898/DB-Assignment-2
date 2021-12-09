import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

sc = SparkContext.getOrCreate()


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
spark = SparkSession(sc)

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df2 = df.filter(F.col("Price Range").isNotNull())

df2max = df2.sort("City",F.col("Price Range").desc(),F.col("Rating").desc())
df2max = df2max.groupBy("City","Price Range").agg(F.max("Rating"),F.first("Name")).sort("City","Price Range").withColumnRenamed("first(Name)","Name").withColumnRenamed("max(Rating)","Rating")

df2min = df2.sort("City",F.col("Price Range").desc(),F.col("Rating"))
df2min = df2min.groupBy("City","Price Range").agg(F.min("Rating"),F.first("Name")).sort("City","Price Range").withColumnRenamed("first(Name)","Name").withColumnRenamed("min(Rating)","Rating")

df2 = df2max.union(df2min)
df = df.join(df2,["Name","City","Price Range","Rating"],'inner').sort("City","Price Range","Rating")
df = df.select("_c0","Name","City","Cuisine Style","Ranking","Rating","Price Range","Number of Reviews","Reviews","URL_TA","ID_TA")

df.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question2/"% (hdfs_nn))





