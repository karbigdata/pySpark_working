from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("aer").master("local[2]").getOrCreate()

myList = [1002, 3001, 4002, 2003, 2002, 3004, 1003, 4006]

df = spark.createDataFrame(myList, IntegerType()).toDF("value")

df = df.withColumn("key", F.col("value") % 1000) \
    .groupBy("key").count()
df.show()

df = df.groupBy("key").agg(F.expr("count(key) as count"), F.expr("sum(key) as sum"))
df.show()