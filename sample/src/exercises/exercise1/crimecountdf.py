from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# create spark session
spark = SparkSession.builder.master("local[*]").appName('crimecount').getOrCreate()


crimedf = spark.read.load("hdfs://localhost:9000/usr/crime/csv",format='csv', header='true',
                          sep=',', inferSchema=True)

# getdate in required format
crimeDfWithDate = crimedf.withColumn('YearMonth',concat(substring(crimedf['Date'], 7, 4),
                                                        substring(crimedf['Date'], 4, 2)))\
    .select(['ID', 'YearMonth', 'Primary Type'])\
    .groupBy("YearMonth", "Primary Type").count()\

crimeDfWithDate.withColumn('count', crimeDfWithDate['count'] * -1)\
    .sort("YearMonth", "count")\
    .show()


