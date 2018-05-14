#spark-submit --master spark://172.21.249.73:7077 gdelt.py

import sys
import time
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) > 1:
	print sys.argv
        print("Usage: gdelt.py")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonGDELT")\
        .getOrCreate()
    df = spark.read.csv('/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv', sep="\t", inferSchema='false', maxColumns=58);
    df.registerTempTable("gdelt");
   # df.printSchema()
   # df.createOrReplaceTempView("gdelt")
   # spark.sql("select * from gdelt").show()

   # for i in range(57, 58):
      #  query="select " + "_c" + str(i) + " from gdelt"
    #    query="select count (  " + "_c" + str(i) + ") from gdelt"
     #   df=spark.sql(query) 
#	df.write.json("logjson", mode='append')
	#df.write.csv("logcsv",mode='append')
 
   # query="select count (_c0) from gdelt where _c34 > 30 " // We have Only 5 records with GlobalEventId for which 'AvgTone' set higher than 30.
    query="select _c57  from gdelt where _c57 IS NOT NULL"
    df=spark.sql(query)
    df.repartition(1).write.csv("URLcsv")
    spark.stop()
