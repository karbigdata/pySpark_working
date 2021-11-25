from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, sum,avg,min,mean,count

spark = SparkSession.builder.appName("pyspark demo").master("local[2]").getOrCreate()
dataDF = spark.read \
    .options(header='True', inferSchema='True') \
    .csv("D:\\BigData\\BigDataProjects\\resources\\StudentData.csv")

dataDF.printSchema()
dataDF.show(5)

groupGenderDF = dataDF.groupby(col("gender")).count()
groupGenderDF.show()

# group on multiple cols
dataDF.groupBy("course").agg(count("*").alias("countOfMarks"), sum("marks")).show()
