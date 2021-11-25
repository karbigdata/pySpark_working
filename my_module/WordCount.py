from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("word count").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\sampleFile4.txt")

print(rddData.collect())

resData = rddData.flatMap(lambda words : words.split(' ')).map(lambda word: (word,1)).reduceByKey(lambda x,y: (x+y))
print(resData.collect())
print(resData.countByValue())
