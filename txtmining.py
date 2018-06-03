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
    df = dfReader.json('/data/wikipedia/articles.json').limit(1) #DataFrame[id: string, text: string, title: string, url: string]
    #df.select("text").show(truncate = False)
    #df.printSchema()
#root                                                                            
# |-- id: string (nullable = true)
# |-- text: string (nullable = true)
# |-- title: string (nullable = true)
# |-- url: string (nullable = true)
#
    #df.show()
    #print type( df) <class 'pyspark.sql.dataframe.DataFrame'>
    
    df_stop = spark.read.text('/data/stopwords.txt')
    stopList = [s.value for s in df_stop.select('value').collect()]
    stopList.extend(["</ref>", "<ref>", "<ref"])
#    print df_stop;
#    df_stop.show()
#    print "count", df_stop.rdd.count()
#    print df_stop.printSchema()
    print "###################################TOKENIZE###################################"
    tokenizer = Tokenizer(inputCol="text",outputCol="words")
    wordsData = tokenizer.transform(df)
    wordsData.show(truncate = False)

    print "###################################STOPWORD REMOVE#############################"
    remover = StopWordsRemover(inputCol="words", outputCol="filteredTxt", stopWords=stopList)
    wordsData_NoStop = remover.transform(wordsData)#.show(truncate=False)
    wordsData_NoStop.select("filteredTxt").show(truncate = False)
    wordsData_NoStop.printSchema()

    # fit a CountVectorizerModel from the corpus.
    #cv = CountVectorizer(inputCol="filteredTxt", outputCol="features", vocabSize=1<<20, minDF=2.0)
    #model = cv.fit(wordsData_NoStop)
    #result = model.transform(wordsData_NoStop)
    #result.show(truncate=False)

    _filteredTxt = wordsData_NoStop.select("filteredTxt").rdd
#    print type(_filteredTxt) #<class 'pyspark.rdd.RDD'>                                                       
    # x:<class 'pyspark.sql.types.Row'>
    # x["filteredTxt"]:<type 'list'> 

    res = _filteredTxt.map(lambda x: x["filteredTxt"]).flatMap(lambda x: x).collect().map(lambda x: None if x=='' else x).reduceByKey(lambda a, b: a + b).collect()
    print res
    print "res", type(res)
  #  result_1 = _filteredTxt.flatMap(lambda x: x).map(lambda x: str(x).split()).reduceByKey(lambda a, b: a + b).collect()
  #  top100 = (sorted(result_2, key=lambda tup: tup[1], reverse=True))[0:100]
  #  for item in top100:
  #      print item[0]
  #      print item[1]

    
    hashingTF = HashingTF(inputCol="filteredTxt", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(wordsData_NoStop)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

#    rescaledData.select("url", "features").show(truncate = False)

"""
|https://en.wikipedia.org/wiki?curid=12 |(20,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19],[72.22659218020823,70.94083035737691,63.523121308444104,73.40963526275935,65.72775185243236,74.98610589761486,61.38190288288243,66.17379088778677,61.754535892336285,58.32915995173227,38.889957285439024,72.42078091431766,2.53475825370568,65.94941911177912,68.19852531496224,61.27781832940704,59.27339450763014,66.46152500922966,101.72944950303025,52.1655046510415])                     |
|https://en.wikipedia.org/wiki?curid=25 |(20,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19],[32.478767361549735,43.931248157091204,26.845152002585714,30.495651479604525,26.854905384402386,26.886910042568214,28.781944892452046,21.95033063594878,16.621781623357805,26.015353030115335,16.654052296352713,29.447360332864573,1.3277305138458324,31.67136135209946,29.93725415324895,33.91385194952932,28.653489288048693,20.838292877003123,46.795546771393916,34.10265678018226])        |
"""

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
