# http://discuss.itversity.com/t/exercise-01-get-monthly-crime-count-by-type/7846

# using spark core api - RDD

from pyspark.sql import SparkSession, SQLContext
# create spark session
spark = SparkSession.builder.appName('crime-count').getOrCreate()
sc = spark.sparkContext
sqc = SQLContext(sc)

# read file -- hdfs host present in core-site.xml file
crimedata = sc.textFile("hdfs://localhost:9000/usr/crime/csv")
# crimedata = sqc.read.load('hdfs://localhost:9000/usr/crime/csv', 'csv')


def getmonth(record):
    crimedate = record.split(",")[2].split(" ")[0]
    print(crimedate)
    crimemonth = crimedate.split("/")[1]
    crimeyear= crimedate.split("/")[2]
    return str(crimeyear+"-"+crimemonth)


count = 0
for data in crimedata.take(5):
    if count > 0:  # skip header
        print(getmonth(data))
    count = count + 1
    if count == 2:
        break
