import sys
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import requests

def search_split(line):
 	if (line.find("//")> -1):
	    domain=line.split("//")[1].split("/")[0]
	    print domain
	    return (domain,1);
	else:
	    return (line,1)

def valid_urls(line):
	print line
        if ( "//" in line):
	    print "A valid URL"
	    domain=line.split("//")[1].split("/")[0]            
	    print domain
	    try:
	        request = requests.get(line)
	    except Exception, e:
		print "Exception"+repr( e)
		return (domain,0);
            if request.status_code == 200:
                works=1
            else:
	        works=0
		print "request.status_code",request.status_code,"return", works
            return (domain,works);
        else:
	    print "A IN valid URL"
            return (line,0)

def readCSV():
    global spark = SparkSession \
        .builder \
        .appName("GDELT_URL") \
        .getOrCreate()

    sc = SparkContext.getOrCreate()
    # Without limit, with all records it gives java.lang.OutOfMemoryError: Java heap space spark
    df = spark.read.csv('url.csv')#.limit(2000);  # <class 'pyspark.sql.dataframe.DataFrame'>
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

    readCSV()
    MR()

    spark.stop()
