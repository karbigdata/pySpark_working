import os

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, StringType


def initSparkSession():
    spark = SparkSession.builder.appName("10Filemerge").master("local[2]").getOrCreate()
    return spark


# # Approach 1
# traverse through dir and create List of 5 files batch
# pathNFileNameRDD = spark.sparkContext.wholeTextFiles("data")
# print("Num of files " + str(pathNFileNameRDD.count()))
#


mySchema = StructType([
    StructField("id", IntegerType()),
    StructField("value", StringType())
])


def buildDataFrameBatch(batchFilesList, spark):
    resDF = spark.read.schema(mySchema).csv(batchFilesList[0])
    for idx in range(1, len(batchFilesList)):
        print("fileNames are", len(batchFilesList), "FileName is ", batchFilesList[idx])
        nextDF = spark.read.schema(mySchema).csv(batchFilesList[idx])
        resDF = resDF.unionAll(nextDF)

    return resDF


def sendBatchFilesInfo(batchFilesList, sparkSC):
    print("list is ", batchFilesList)
    listDataDFInt = buildDataFrameBatch(batchFilesList, sparkSC)

    for i in listDataDFInt:
        print("type is ", type(listDataDFInt), " data inside is ", type(listDataDFInt[0]))

    return listDataDFInt


def printDFInLoop(DataFramesInList):
    for df in DataFramesInList:
        df.show()


if __name__ == "__main__":
    sparkSC = initSparkSession()
    batchSize = 4  # list starts with 0
    batchSizeNew = 5  # list starts with 0
    batchFiles = []  # empty List
    DFsInList = []
    DataFramesInList = []
    dirPath = "data/"
    fileListInDir = os.listdir(dirPath)
    # print("files in dir ", fileListInDir)
    # print("files in dir ", len(fileListInDir))
    idx = 0
    numOfBatches = int(len(fileListInDir)/batchSizeNew) + 1
    fileInBatch = fileListInDir[0:batchSizeNew:]
    print("num of batch files ", numOfBatches)
    runningCount = 0
    while idx < numOfBatches:
        print("idx is ", idx)
        count = 0
        while count < batchSizeNew and runningCount < len(fileListInDir):
            batchFiles.append(dirPath + fileListInDir[runningCount])
            count = count + 1
            print("running count ", runningCount)
            runningCount = runningCount + 1

        DataFramesInList.append(sendBatchFilesInfo(batchFiles, sparkSC))
        idx = idx + 1
        print("batch files are", batchFiles)
        batchFiles.clear()

    printDFInLoop(DataFramesInList)

# Approach 2
    # for fileName in fileListInDir:
    #     if count < batchSize:
    #         batchFiles.append(dirPath + fileName)
    #         count = count + 1
    #     else:
    #         batchFiles.append(dirPath + fileName)
    #         sendBatchFilesInfo(batchFiles, sparkSC)
    #         print("resetting count 0", end='\n')
    #         batchFiles.clear()
    #         count = 0
    #
    # # sending remaining filesList
    # sendBatchFilesInfo(batchFiles, sparkSC)
