from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\MonthData.txt")

print(rddData.collect())

rddData2 = rddData.map(lambda x: (x.split(',')[1], float(x.split(',')[2])))
print(rddData2.collect())


rddData3 = rddData2.reduceByKey(lambda x,y : (x if x < y else y))
print(rddData3.collect())


def func(myList):
    print(list(myList[1]))
    listKey = myList[0]
    listVals = list(myList[1])
    mi = 0
    mx = 0
    for ele in listVals:
      if mx < ele:
          mx = ele
          mi = mx
      else:
          mi = ele

    print("min max is " + str(mi) + " " + str(mx))
    return (listKey, (mi, mx))


rddDataGrp = rddData2.groupByKey().map(func) #.mapValues(list)
print(rddDataGrp.collect())