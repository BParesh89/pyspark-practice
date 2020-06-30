# using spark core api - RDD

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
# create spark session
spark = SparkSession.builder.appName('nyse-parquet').getOrCreate()
sc = spark.sparkContext

# defind custom schema to read data
customSchema = StructType([StructField("stocketicker", StringType(), True),
                           StructField("transactiondate", StringType(), True),
                           StructField("openprice", FloatType(), True),
                           StructField("highprice", FloatType(), True),
                           StructField("lowprice", FloatType(), True),
                           StructField("closeprice", FloatType(), True),
                           StructField("volume", IntegerType(), True)])

# read file with mode - DROPMALFORMED and custome schema defined with header=true to get rid of header
# which doesn't have schema
nysedata = spark.read.load("/home/vagrant/Downloads/data-master/nyse/nyse_data.tar.gz", format='csv', header=True,
                          sep=',', mode='DROPMALFORMED', schema=customSchema)
# write to parquet file
nysedata.write.parquet("/home/vagrant/Downloads/data-master/nyse_parquet")
