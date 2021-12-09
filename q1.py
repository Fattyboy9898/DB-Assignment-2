import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.context import SparkContext
from pyspark.sql import functions as F

sc = SparkContext.getOrCreate()


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
spark = SparkSession(sc)

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df=df.withColumn('Rating',df['Rating'].cast("float"))
df = df.filter((df['Rating'] >= 1) & (df['Rating'].isNotNull()) )
df =df.filter(df["Reviews"] != F.lit(""))
df =df.filter(df["Reviews"].isNotNull())
df.show()
print(df.count())

df.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question1/answer.csv"% (hdfs_nn))

