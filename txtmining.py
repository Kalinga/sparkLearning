#Spark version 2.1.0
#Scala version 2.11
#Java HotSpot(TM) 64 - Bit Server VM, 1.8 .0_102

# Spark Context: Prior to Spark 2.0.0 sparkContext was used as a channel to access all spark
# SPARK 2.0.0 onwards, SparkSession provides a single point of entry to interact with underlying
# Spark functionality and allows programming Spark with DataFrame and Dataset APIs.
# All the functionality available with sparkContext are also available in sparkSession.

import sys
from pyspark.sql import SparkSession

def readJSON():
    global spark = SparkSession \
        .builder \
        .appName("wiki_articles") \
        .getOrCreate()

    # Once the SparkSession is instantiated, we can configure Spark’s run-time config properties.
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    #spark.conf.set("spark.executor.memory", "2g")

    dfReader = sparkSession.read
    df = dfReader.json('/data/wikipedia/articles.json')
    #df.show(df.count())
    print "count", df.count

def MR():
    df_rdd = df.rdd  # <class 'pyspark.rdd.RDD'>
    result_1 = df_rdd.flatMap(lambda x: x)  # <class 'pyspark.rdd.PipelinedRDD'>
    print type(result_1)
    result_2 = result_1.map(lambda x: str(x)).map(valid_urls).reduceByKey(lambda a, b: a + b).collect()
    top100 = (sorted(result_2, key=lambda tup: tup[1], reverse=True))[0:100]
    thefile = open('validURLCount100Domain.txt', 'w')
    for item in top100:
        print item[0]
    print item[1]
    contents = item[0] + ":" + str(item[1]) + "\n"
    thefile.write("%s" % contents)

if __name__ == "__main__":
    if len(sys.argv) > 1:
	print sys.argv
        print("Usage: url.py")
        sys.exit(-1)

    readJSON()
    #MR()

    spark.stop()
