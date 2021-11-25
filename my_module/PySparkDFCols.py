from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import column

spark = SparkSession.builder.appName("pyspark demo").master("local[2]").getOrCreate()
dataDF = spark.read \
    .options(header='True', inferSchema='True') \
    .csv("D:\\BigData\\BigDataProjects\\resources\\StudentData.csv")

dataDF.printSchema()
dataDF.show(5)

# add 10 marks to col('marks')
# dataDF.withColumn("marks", col('marks') + 10).show(5)

# create new col and minus 10 marks
# dataDF.withColumn("agg marks", col('marks') - 10).show(5)

# create a new col and provide static val
# dataDF.withColumn("country", lit('USA')).show(5)

# rename the col marks to marksObtained
# dataDF.withColumnRenamed("marks", "marks Obtained").show(5)

# dataDF.select(col("name").alias("FullName")).show(5)

# filter/Where cond on row level
# filter students who enrolled for Database(DB)
# dataDF.filter(col("course") == "DB").show(3)

# filter DB and marks > 50
# dataDF.filter((col("course") == "DB") & (col("marks") > 50)).show()

dataDF.filter(col("course").startswith("D")).show(3)

 