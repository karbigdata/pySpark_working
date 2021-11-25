from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\sampleFile5.txt")

print(rddData.collect())

# avg of each movie
avgRdd = rddData.map(lambda line: (line.split(',')[0], (int(line.split(',')[1]), 1)))\
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))  #.map(lambda x:(x[0], x[1][0]/x[1][1]))

resRdd = avgRdd.map(lambda x: (x[0], round(x[1][0]/x[1][1], 2)))
#.mapValues(lambda x,y: x+y)  # .map(lambda x: x.split(',')).map(lambda m, r: (m, r))
print(avgRdd.collect())
print(resRdd.collect())
