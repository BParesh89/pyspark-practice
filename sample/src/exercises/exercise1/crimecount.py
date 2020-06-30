# http://discuss.itversity.com/t/exercise-01-get-monthly-crime-count-by-type/7846

# using spark core api - RDD

from pyspark.sql import SparkSession, SQLContext
# create spark session
spark = SparkSession.builder.appName('crime-count').getOrCreate()
sc = spark.sparkContext

# read file -- hdfs host present in core-site.xml file
crimedata = sc.textFile("hdfs://localhost:9000/usr/crime/csv")


crimefirst = crimedata.first()

# remove header record , add number of fields in each row and filter records not having 22 fields to get valid records
crimerdd = crimedata.filter(lambda rec: rec != crimefirst)\
                    .map(lambda rec: ",".join([str(len(rec.split(","))), rec]))\
                    .filter(lambda rec: rec.split(",")[0] == '22')


def getmonth(record):
    """
    function to extract yearmonth in YYYYMM format
    """
    crimedate = record.split(",")[3].split(" ")[0]
    crimemonth = crimedate.split("/")[1]
    crimeyear = crimedate.split("/")[2]
    return str(crimeyear+crimemonth)


# create new rdd with tuple having yearmonth ,crime type and append 1 at end of each record which we can use in groupby
crimerddwithMonth = crimerdd.map(lambda rec: ((getmonth(rec), rec.split(",")[6]), 1))

# get count per month per type
crimerddgroupbyMonthtype = crimerddwithMonth.reduceByKey(lambda a, b: a + b)

# get yearmonth and count in one tuple(count as negative) to sort in yearmonth ascending and count as descending
# here in rdd created after map, first field is key which is tuple of yearmonth and negated count,
# second field is actual record which is tab separated string
crimerddfinalsorted = crimerddgroupbyMonthtype\
    .map(lambda rec: ((int(rec[0][0]), -rec[1]), str(rec[0][0]) +'\t' + str(rec[1]) + '\t' + rec[0][1]))\
    .sortByKey()\
    .map(lambda rec: rec[1])  # remove key

# write file in compressed format in hdfs
crimerddfinalsorted.saveAsTextFile(path="hdfs://localhost:9000/usr/crime/solution_1",
                                   compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
# see data by print first 5 rows
for row in crimerddfinalsorted.take(5):
    print(row)


