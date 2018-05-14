#spark-submit --master spark://172.21.249.73:7077 gdelt.py

import sys
import time
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
def search_split(line):
 	if (line.find("//")> -1):
	    domain=line.split("//")[1].split("/")[0]
	    print domain
	    return (domain,1);
	else:
	    return (line,1)

if __name__ == "__main__":
    if len(sys.argv) > 1:
	print sys.argv
        print("Usage: url.py")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("GDELT_URL")\
        .getOrCreate()
    
    sc = SparkContext.getOrCreate() 
    df = sc.textFile('tenRecords.txt');

    result=df.map(search_split).reduceByKey(lambda a, b: a + b).collect()
    print result
    thefile = open('domainCount.txt', 'w')
    for item in result:
       thefile.write("%s\n" % item[0]+":"+ str(item[1])+"\n")
    spark.stop()
