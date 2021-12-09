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

#df=df.withColumn('Number of Reviews',df['Number of Reviews'].cast("float"))
df=df.withColumn('Rating',df['Rating'].cast("float"))
newdf = df.filter((df['Rating'] >= 1) & (df['Rating'].isNotNull()) )
newdf = newdf.filter(newdf["Reviews"] != F.lit("[ [  ], [  ] ]"))
newdf.show()
print(newdf.count())

newdf.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question1/answer.csv"% (hdfs_nn))

