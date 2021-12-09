import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import explode,col
from pyspark.sql.functions import count
from pyspark.sql.types import StructField, StructType,StringType,ArrayType,IntegerType,DoubleType,BooleanType

sc = SparkContext.getOrCreate()


# don't change this line. 
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
def strip(data):
    city = data.City
    cuisine = data["Cuisine Style"]
    cuisine = cuisine.strip()
    return (city,cuisine[1:-1])
df = df.withColumn('Cuisine Style',F.regexp_replace('Cuisine Style',"\\[",""))
df = df.withColumn('Cuisine Style',F.regexp_replace('Cuisine Style',"\\]",""))
df = df.withColumn('Cuisine Style',F.split(F.col('Cuisine Style'),","))
df_exploded = df.withColumn('Cuisine Style',explode('Cuisine Style'))
newdf = df_exploded.select("City","Cuisine Style")
rdd = newdf.rdd.map(lambda x : strip(x))
newdf = spark.createDataFrame(rdd).toDF("City","Cuisine Style")
newdf = newdf.groupBy("City", "Cuisine Style").agg(count("*").alias("count"))
#newdf = newdf.select(col("City").alias("City"),col("Cuisine Style").alias("Cuisine"),col("count").alias("count"))
newdf = newdf.sort("City","Cuisine Style")
newdf.show()
print(newdf.count())

newdf.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question4/answer.csv"% (hdfs_nn))
