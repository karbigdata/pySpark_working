from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\sampleFile.txt")

print(rddData.collect())

# filterData = rddData.filter(lambda x: [s for s in x.split(' ') if s == '5'])
# print(filterData.collect())

textData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\sampleFile3.txt")

print(textData.collect())


def rowData(x):
    l = x.split(' ')
    l2 = []
    for s in l:
        if s[0] != 'a' or s[0] != 'c':
            l2.append(s)

    return l2


resData = textData.map(rowData)
print(resData.collect())

resData2 = textData.flatMap(lambda line: [s for s in line.split(' ') if s[0] != 'a' and s[0] != 'c'])
print(resData2.collect())

resData3 = textData.flatMap(lambda line: line.split(' '))
filterData = resData3.filter(lambda word: word[0] != 'a' and word[0] != 'c')
print(filterData.collect())

resData4 = textData.flatMap(lambda line: line.split(' ')).filter(lambda word: not (word.startswith('a') or word.startswith('c')))
print(resData4.collect())
