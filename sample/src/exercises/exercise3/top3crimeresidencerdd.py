# using spark core api - RDD

from pyspark.sql import SparkSession, SQLContext
# create spark session
spark = SparkSession.builder.appName('crime-count').getOrCreate()
sc = spark.sparkContext

# read file -- hdfs host present in core-site.xml file
crimedata = sc.textFile("hdfs://localhost:9000/usr/crime/csv")
# get header record
crimefirst = crimedata.first()
#print("Total records in file :", crimedata.count())

# remove header record , add number of fields in each row and filter records not having 22 fields to get valid records
crimerdd = crimedata.filter(lambda rec: rec != crimefirst) \
                    .map(lambda rec: ",".join([str(len(rec.split(","))), rec])) \
                    .filter(lambda rec: rec.split(",")[0] == '22')\
                    .filter(lambda rec: rec.split(",")[8] == 'RESIDENCE')\
                    .map(lambda rec: (rec.split(",")[6], 1))\
                    .reduceByKey(lambda a, b: a + b)\
                    .map(lambda rec: (rec[1], rec[0]))\
                    .sortByKey(False)

for data in crimerdd.take(5):
    print(data)

#print("Total records in file :", crimerdd.count())
#crimeresidence = crimedata.filter(lambda rec: rec.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[])