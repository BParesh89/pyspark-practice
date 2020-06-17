# demo to connect to remote db2 non ssl port server

# to run it via spark-submit use -- $SPARK_HOME/bin/spark-submit <relative path of file from folder of running cmd>

from pyspark.sql import SparkSession, SQLContext

if __name__ == "__main__":
    # create spark session
    spark = SparkSession.builder.appName('hive').getOrCreate()
    spark.read.format("jdbc").\
        option("url", "jdbc:db2://meuba.vipa.uk.ibm.com:446/EUBADB2A").\
    option("dbtable", "CTMPL.CTMTZTR").\
    option("driver","com.ibm.db2.jcc.DB2Driver").\
    option("user", "in42322").\
    option("password", "feb14epr").load().show()