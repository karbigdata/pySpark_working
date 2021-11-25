import os

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

spark = SparkSession.builder.appName("10Filemerge").master("local[2]").getOrCreate()

# # Approach 1
# # get the files count and itr 10 files and copy to list
# pathNFileNameRDD = spark.sparkContext.wholeTextFiles("data")
# print("Num of files " + str(pathNFileNameRDD.count()))
#
dirPath = "data/"
fileListInDir = os.listdir(dirPath)
print("files in dir ", fileListInDir)
print("files in dir ", len(fileListInDir))

mySchema = StructType([
    StructField("id", IntegerType()),
    StructField("value", StringType())
])

batchSize = 5
batchFiles = []  # empty List
idx = 0
count = 0
for fileName in fileListInDir:
    if count < batchSize:
        # dataDF = spark.read.schema(mySchema).load(dirPath + fileName)
        batchFiles.append(dirPath+fileName)
        count = count + 1
    else:

        print("resetting count 0", end='\n')
        count = 0

