from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# create spark session
spark = SparkSession.builder.master("local[*]").appName('crimecount').getOrCreate()


crimedf = spark.read.load("hdfs://localhost:9000/usr/crime/csv",format='csv', header='true',
                          sep=',', inferSchema=True)
crimecountdf = crimedf.filter(crimedf['Location Description'] == 'RESIDENCE')\
       .select(['ID', 'Primary Type'])\
       .groupBy('Primary Type').count()

crimecountdf.sort('count', ascending=False).show(3)