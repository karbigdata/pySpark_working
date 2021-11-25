from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, sum, avg, min, max, mean, count, udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("pyspark demo").master("local[2]").getOrCreate()
dataDF = spark.read \
    .options(header='True', inferSchema='True') \
    .csv("D:\\BigData\\BigDataProjects\\resources\\OfficeDataProject.csv")

dataDF.printSchema()
dataDF.show(5)
# Print the total num of employees
print("Total num of employees are ", dataDF.count())

# num of depts
# dataDF.select("department").distinct().show()
numDepts = dataDF.select("department").distinct().count()
print("num of departments are ", numDepts)

# print the names of the department
dataDF.select("department").distinct().show()

# print the total num of emps in each dept
print("====== total num of emps per dept =============", end="\n")
totalEmpsPerDept = dataDF.groupby(col("department")).count()
totalEmpsPerDept.show()

# print the total num of emps in each state
print("====== total num of emps in each state ==========")
totalEmpPerState = dataDF.groupby(col("state")).count()
totalEmpPerState.show()

# print the total num of emps in each state in each dept
totalEmpPerStatePerDept = dataDF.groupby(col("state"), col("department")).count()
totalEmpPerStatePerDept.show()

# print the min and max salaries in each dept and sort salaries in ascending order
print("===== min and max sals per dept ======", end="\n")
minMaxSalsPerDept = dataDF.groupBy("department").agg(min("salary").alias("maxSals"), max("salary").alias("minSals")) \
    .orderBy(col("minSals").asc())
minMaxSalsPerDept.show()

# print the names of emps in NY under Finance dept whose bonuses are gt avg bounses of emp in NY state
avgBonusNY = dataDF.filter(col("state") == "NY").groupBy(col("state")).agg(avg("bonus").alias("avg_bonus")) \
    .select("avg_bonus").collect()[0]['avg_bonus']
print(avgBonusNY)

empNameNYnFin = dataDF.filter((col("state") == "NY") & (col("department") == "Finance")) \
    .withColumn("avg_ny_bonus", lit(avgBonusNY))
empNameNYnFin = empNameNYnFin.filter(col("bonus") > col("avg_ny_bonus"))
empNameNYnFin.show()

# raise the salary hike by 500$ for all emp whose age is > 45
salRaiseDF = dataDF.filter(col("age") > 45).withColumn("raisedSal", col("salary") + 500)
salRaiseDF.show()


def incrSal(age, currSal):
    if age > 45:
        return currSal + 500
    return currSal


incr_sal_udf = udf(lambda x, y: incrSal(x, y), IntegerType())

salRaiseUDFDF = dataDF.withColumn("temp_salary", incr_sal_udf(col("age"), col("salary"))).show()

# create DF all emps age > 45 and save them to file
# empAge45DF = dataDF.filter(col("age") > 45)
# empAge45DF.show()
# empAge45DF.write.option("header", True).csv("D:\\BigData\\BigDataProjects\\resources\\output")
