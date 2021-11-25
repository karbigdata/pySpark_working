from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rddData = spark.sparkContext.textFile("D:\\BigData\\BigDataProjects\\resources\\StudentData.csv")
print(rddData.collect())

# Filtering first n records
filterFirstNRecords = rddData.zipWithIndex().filter(lambda x: x[1] > 2)
print(filterFirstNRecords.collect())

# header store in list
headRddDataStr = rddData.first()
print(headRddDataStr)

# Filter out the first row
realRddData = rddData.filter(lambda x: x != headRddDataStr)
print(realRddData.collect())

# show the num of students in the file
# Show the total marks achieved by Female and Male
# Show total num of students that have passed and failed , 50+ marks are considered as PASS
# show the total num of students enrolled per course
# show the total marks that students have achieved per course
# show the avg marks that students have achieved per course
# show the min and max marks achieved per course
# show the avg ags of male and female students

# show the num of students in the file
print(realRddData.count())
totalNumStudents = realRddData.count()

# Show the total marks achieved by Female and Male
totalMarksFemMale = realRddData.map(lambda x: (x.split(',')[1], int(x.split(',')[5]))).reduceByKey(lambda x, y: x+y)
print(totalMarksFemMale.collect(), end="\n")

# Show total num of students that have passed and failed , 50+ marks are considered as PASS
totalNumStudentsPassed = realRddData.filter(lambda x: int(x.split(',')[5]) > 50)
print("Total Num of passed Students " + str(totalNumStudentsPassed.count()))
totalNumStudentsFailed = realRddData.filter(lambda x: int(x.split(',')[5]) <= 50)
print("Total Num of failed Students " + str(totalNumStudentsFailed.count()))
print("Total Passed - Failed " + str((totalNumStudents - totalNumStudentsPassed.count())))

# show the total num of students enrolled per course
totalNumOfCourseEnrollment = realRddData.map(lambda x: (x.split(',')[3], 1)).reduceByKey(lambda x, y: x + y)
print(totalNumOfCourseEnrollment.collect())

# show the total marks that students have achieved per course
# show the avg marks that students have achieved per course
# show the min and max marks achieved per course
# show the avg ags of male and female students

# show the total marks that students have achieved per course
totalMarksAchievedPerCourse = realRddData.map(lambda x: (x.split(',')[3], int(x.split(',')[5]))).reduceByKey(lambda x,y: x+y)
print(totalMarksAchievedPerCourse.collect())

# show the avg marks that students have achieved per course
avgMarksAchievedPerCourse = realRddData.map(lambda x: (x.split(',')[3], (int(x.split(',')[5]), 1)))
totalMarksAndCourseCount = avgMarksAchievedPerCourse.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
print(totalMarksAndCourseCount.collect())
avgMarksPerCourse = totalMarksAndCourseCount.map(lambda x: (x[0], round(x[1][0]/x[1][1], 2)))
print(avgMarksPerCourse.collect())

# show the min and max marks achieved per course
minMarksPerCourse = realRddData.map(lambda x: (x.split(',')[3], int(x.split(',')[5])))
print(minMarksPerCourse.collect() )
minMarksRdd = minMarksPerCourse.reduceByKey(lambda x, y: (x if x < y else y))
print("min marks per course ", minMarksRdd.collect())

maxMarksPerCourse = realRddData.map(lambda x: (x.split(',')[3], int(x.split(',')[5])))
print(maxMarksPerCourse.collect())
maxMarksPerCourseRDD = maxMarksPerCourse.reduceByKey(lambda x, y: (x if x > y else y))
print("max marks per course ", maxMarksPerCourseRDD.collect())
# show the avg age of male and female students
totalAgeAndCountOfMaleAndFemale = realRddData.map(lambda x: (x.split(',')[1], (int(x.split(',')[0]), 1)))\
     .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
print(totalAgeAndCountOfMaleAndFemale.collect())
avgAgeOfMalesAndFemales = totalAgeAndCountOfMaleAndFemale.map(lambda x: (x[0], round(x[1][0]/x[1][1],2)))
print(avgAgeOfMalesAndFemales.collect())

