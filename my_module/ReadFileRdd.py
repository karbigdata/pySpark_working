from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\sampleFile2.txt")
print(rddData.collect())


def foo(x):
    l = x.split(' ')
    #print(rowData)
    l2 = []
    for s in l:
        #print(ele)
        l2.append(len(s))

    return l2


resRddData = rddData.map(foo)
print(resRddData.collect())

resRddData2 = rddData.map(lambda x: [len(s) for s in x.split(' ')])
print(resRddData2.collect())

resRddData3 = rddData.flatMap(lambda x: [len(s) for s in x.split(' ')])
print(resRddData3.collect())