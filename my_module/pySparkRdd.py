from pyspark.sql import SparkSession


def foo(x):
    l = x.split(' ')
    l2 = []
    for s in l:
        l2.append(int(s) * 2)

    return l2


spark = SparkSession.builder.appName("rdd").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\sampleFile.txt")

print(rddData.collect())

rddData2 = rddData.map(lambda x: x.split(" ") * 2)
# .map(lambda y: y*2)

print(rddData2.collect())

rddData3 = rddData.map(foo)
print(rddData3.collect())

rddData4 = rddData.map(lambda x: [(2*int(s)) for s in x.split(' ')])
print(rddData4.collect())

rddData5 = rddData.flatMap(lambda x: [(2+int(s)) for s in x.split(' ')])
print(rddData5.collect())