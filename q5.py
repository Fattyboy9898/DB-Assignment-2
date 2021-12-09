import sys 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
def extractName(data):
    print(data)
    movie_id = data.movie_id
    title = data.title
    cast = data.cast
    index = cast.find('"name"') + 9
    cast = cast[index:]
    cast = cast.strip()
    return (movie_id,title,cast)

df =df.drop("crew")
df = df.withColumn("cast",F.split(df['cast'],'\\}, \\{"cast_id"'))
df = df.withColumn("cast",F.explode("cast"))
df = df.withColumn("cast",F.split(df['cast'],'", "'))
df = df.withColumn("cast",F.explode("cast"))
df = df.filter(F.col("cast").contains("name"))
rdd = df.rdd.map(lambda x: extractName(x))
df = spark.createDataFrame(rdd).toDF("movie_id","title","actor")
df = df.sort("actor")

df2 = df.groupBy('movie_id','title').agg(F.collect_list('actor')).sort('movie_id')
df2 = df2.select('movie_id','title',F.explode('collect_list(actor)').alias('actor'))

df2 = df2.distinct()
df3 = df2.alias('df3')
df4 = df2.alias('df4')
df3 = df3.withColumnRenamed("actor","actor1")
df4 = df4.withColumnRenamed("actor","actor2")
dfForJoining = df3.join(df4,[F.col("df3.movie_id")==F.col("df4.movie_id"),F.col("actor1")<F.col("actor2")])
df =dfForJoining.groupBy("actor1","actor2").agg(F.count("df3.movie_id"))

df = df.filter(df["`count(df3.movie_id)`"]>1)

dfForJoining = dfForJoining.select("df3.movie_id","df3.title","actor1","actor2")
dfForJoining = dfForJoining.withColumnRenamed("actor1","joinActor1")
dfForJoining = dfForJoining.withColumnRenamed("actor2","joinActor2")

df = df.join(dfForJoining,[F.col("actor1")==F.col("joinActor1"),F.col("actor2")==F.col("joinActor2")])
df = df.select('movie_id','title','actor1','actor2')

df.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question5/answer.csv"% (hdfs_nn))