import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType,StringType,ArrayType,IntegerType,DoubleType,BooleanType

sc = SparkContext.getOrCreate()


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

split_cols = F.split(df["Reviews"],'\\], \\[')
df = df.withColumn('review',split_cols.getItem(0)).withColumn('date',split_cols.getItem(1))

df = df.withColumn('review',F.split(F.col('review'),","))
df = df.withColumn('date',F.split(F.col('date'),","))

newdf = df.withColumn("new",F.arrays_zip("review","date"))\
    .withColumn("new",F.explode("new"))\
        .select("ID_TA",F.col("new.review").alias("review"),F.col("new.date").alias("date"))
newdf = newdf.withColumn('review',F.regexp_replace('review',"'",""))
newdf = newdf.withColumn('date',F.regexp_replace('date',"'",""))
newdf = newdf.withColumn('review',F.regexp_replace('review',"\\[",""))
newdf = newdf.withColumn('date',F.regexp_replace('date',"\\]",""))
newdf.show()

newdf.printSchema()

newdf.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question3/"% (hdfs_nn))




