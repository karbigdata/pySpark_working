from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, sum, avg, min, max, mean, count, udf, broadcast
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("pyspark demo").master("local[2]").getOrCreate()
dataDF = spark.read \
    .options(header='True', inferSchema='True') \
    .csv("D:\\BigData\\BigDataProjects\\resources\\OfficeDataProject.csv")

mySchema = dataDF.schema
dataDF.printSchema()
dataDF.show(5)

# dataDF.repartition(5).write.format("csv").mode("overwrite").save("D:\\BigData\\BigDataProjects\\resources\\partitionData")

smallDF = spark.read.schema(mySchema) \
    .csv("D:\\BigData\\BigDataProjects\\resources\\partitionData\\part-00000-913b6a1b-4c32-42ef-8572-aace58851c0d-c000.csv")
smallDF.show()

brodJoinedDF = dataDF.join(broadcast(smallDF), dataDF.employee_id == smallDF.employee_id)
brodJoinedDF.show()
brodJoinedDF.explain(True)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1")
spark.conf.set("spark.sql.join.preferSortMergeJoin", True)

defJoinedDF = dataDF.join(smallDF, dataDF.employee_id == smallDF.employee_id)
defJoinedDF.show()
defJoinedDF.explain(True)
