#Spark version 2.1.0
#Scala version 2.11
#Java HotSpot(TM) 64 - Bit Server VM, 1.8 .0_102

# Spark Context: Prior to Spark 2.0.0 sparkContext was used as a channel to access all spark
# SPARK 2.0.0 onwards, SparkSession provides a single point of entry to interact with underlying
# Spark functionality and allows programming Spark with DataFrame and Dataset APIs.
# All the functionality available with sparkContext are also available in sparkSession.

import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.ml.feature import StopWordsRemover,CountVectorizer
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession \
        .builder \
        .appName("wiki_articles") \
        .getOrCreate()

sc = spark.sparkContext 

#sqlContext = SQLContext(sc)
#broadcastVar = sc.broadcast(stopList)
#udfStopFilter=udf(stopFilter, StringType())
#spark.udf.register("udfStopFilter", stopFilter)

def readJSON():
    global spark;
    global broadcastVar
    # Once the SparkSession is instantiated, we can configure Spark's run-time config properties.
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    #spark.conf.set("spark.executor.memory", "2g")

    dfReader = spark.read
    df = dfReader.json('/data/wikipedia/articles.json').limit(10) #DataFrame[id: string, text: string, title: string, url: string]
    #df.show(df.count())
    #df.show()
    print type( df)
    
    df_stop = spark.read.text('/data/stopwords.txt')
    stopList = [s.value for s in df_stop.select('value').collect()]
#    print df_stop;
#    df_stop.show()
#    print "count", df_stop.rdd.count()
#    print df_stop.printSchema()

    tokenizer = Tokenizer(inputCol="text",outputCol="words" )
    wordsData = tokenizer.transform(df)
    wordsData.show(truncate = False)

    remover = StopWordsRemover(inputCol="words", outputCol="filteredTxt", stopWords=stopList)
    wordsData_NoStop = remover.transform(wordsData)#.show(truncate=False)
#    processedDF.select("filteredTxt")#.show(truncate = False)

    # fit a CountVectorizerModel from the corpus.
    #cv = CountVectorizer(inputCol="words", outputCol="features", vocabSize=1<<20, minDF=2.0)
    #model = cv.fit(df)
    #result = model.transform(df)
    #result.show(truncate=False)
    
    hashingTF = HashingTF(inputCol="filteredTxt", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(wordsData_NoStop)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    rescaledData.select("url", "features").show(truncate = False)

    # TFIDF, short for term frequency-inverse document frequency
#    result = processedDF.rdd.flatMap(lambda x: x["filteredTxt"]).flatMap(lambda x: x.split()).map(lambda x: (str(x),1)).reduceByKey(lambda a, b: a + b).collect()
#    result = processedDF.rdd.flatMap(lambda x: x["filteredTxt"]).map(lambda x: x.split()).map(lambda x: (str(x),1)).reduceByKey(lambda a, b: a + b).collect()
#    result = processedDF.rdd.flatMap(lambda x: x["filteredTxt"]).map(lambda x: x.split()).map(lambda x: (''.join(x),1)).reduceByKey(lambda a, b: a + b).collect()
#    print type(processedDF.rdd.flatMap(lambda x: x["filteredTxt"]).collect())
#    print type(processedDF.rdd.flatMap(lambda x: x["filteredTxt"]).map(lambda x: x.split()).collect())
#    print type (processedDF.rdd.flatMap(lambda x: x["filteredTxt"]).map(lambda x: x.split()).map(lambda x: (x,1)).collect())
#    print result.count()
#    print type(result)

#    print "Top 10 frequently word"
#    top10 = (sorted(result, key=lambda tup: tup[1], reverse=True))[0:10]
#    print type(top10)
#    print top10


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
        print("Usage: .py")
        sys.exit(-1)

    readJSON()
    #MR()

    spark.stop()
