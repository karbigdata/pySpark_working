from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\MonthData.txt")

print(rddData.collect())

rddData2 = rddData.map(lambda x: (x.split(',')[0], ( float(x.split(',')[2]), 1)))  # .reduceByKey(lambda x,y: )
rddData3 = rddData2.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
print(rddData2.collect())
print(rddData3.collect())
resData = rddData3.map(lambda x: (x[0], x[1][0]/x[1][1]))
print(resData.collect())
