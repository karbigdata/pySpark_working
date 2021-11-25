from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,round

spark = SparkSession.builder.appName("pyspark demo").master("local[2]").getOrCreate()
dataDF = spark.read \
    .options(header='True', inferSchema='True') \
    .csv("D:\\BigData\\BigDataProjects\\resources\\StudentData.csv")

dataDF.printSchema()
dataDF.show(5)

# added total marks with lit(120)
dataDF = dataDF.withColumn("total marks", lit(120))
dataDF.show()

# create new col avg, marks/total marks
dataDF = dataDF.withColumn("avg marks", round((col("marks")/col("total marks"))*100, 2))
dataDF.show()

# filter out >80% in OOP
oopEightyPercentDataDF = dataDF.filter((col("avg marks") >= 80) & (col("course") == "OOP"))
oopEightyPercentDataDF.show()

# filter >60% in Cloud
cloudSixtyPercentDataDF = dataDF.filter((col("course") == "Cloud") & (col("avg marks") >= 60))
cloudSixtyPercentDataDF.show()


