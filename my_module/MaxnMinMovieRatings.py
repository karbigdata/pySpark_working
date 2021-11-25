from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\MovieRatingData.txt")

print(rddData.collect())

rddData2 = rddData.map(lambda x: (x.split(',')[0], (int(x.split(',')[1]),0)))
#print(rddData2.collect())
rddData3 = rddData2.reduceByKey(lambda x,y: (x[0] if x[0] > y[0] else y[0]))
#print(rddData3.collect())


def func(x):
    print(x)
    return True


rddData4 = rddData2.map(func)

print(rddData4.collect())
