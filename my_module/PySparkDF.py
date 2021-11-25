
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("pyspark demo").master("local[2]").getOrCreate()

mySchema = StructType([
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("name", StringType()),
    StructField("course", StringType()),
    StructField("roll", StringType()),
    StructField("marks", IntegerType()),
    StructField("email", StringType())
])

dataDF = spark.read \
    .option("header","true") \
    .schema(mySchema) \
    .csv("D:\\BigData\\BigDataProjects\\resources\\StudentData.csv")

dataDF.printSchema()

# create DF from RDD
rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\StudentData.csv")
header = rddData.first()
realRddData = rddData.filter(lambda x: x != header).map(lambda x: x.split(','))
realRddData = realRddData.map(lambda x: (int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6]))
print(realRddData.collect())

cols = header.split(',')
dataDFFromRdd = realRddData.toDF(cols)
dataDFFromRdd.show(5)
dataDFFromRdd.printSchema()

# adding header, create DF from RDD
dfRdd = spark.createDataFrame(realRddData, mySchema)
dfRdd.printSchema()
dfRdd.show(3)

