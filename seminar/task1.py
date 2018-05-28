import sys
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import catalog

spark = SparkSession \
        .builder \
        .appName("Task1 SS 2018") \
        .getOrCreate()


def readCSV():
    global spark;
    sc = SparkContext.getOrCreate()
    df = spark.read.csv('url.csv')#.limit(2000);  # <class 'pyspark.sql.dataframe.DataFrame'>
    #df.show(df.count())
    # print "count", df.count()
    print df.rdd.partitions.size

def createDB():
    spark.sql("create database database_kalinga")
    print spark.catalog.listDatabases()

def createTB():
    q = """CREATE TABLE testtable(country STRING, captital STRING, population INT)  USING PARQUET PARTITIONED BY (population)"""

#    q = "CREATE TABLE testtable(alpha_2 STRING, alpha_3 STRING, area INT, captital STRING, continent STRING, currency_code STRING ,currency_name STRING, eqivalent_fips_code STRING, fips STRING, geoname_id STRING, languages STRING, name STRING, neighbours STRING, numeric INT, phone INT, population INT, postal_code_format STRING, postal_code_regex STRING, tld STRING)"
 
    spark.sql(q)
    spark.sql("insert into  testtable values('india', 'd', 100)")

    spark.sql("DESCRIBE testtable")
    print spark.catalog.listTables()

def populateTB():
    df = spark.read.csv('countries.csv', header=True,sep=';').limit(20);
    #print("###########################COLS#####################################")
    #print df.columns
    #df.printSchema()
    #print("################################################################")

    view = df.createOrReplaceTempView("countries")
    #print type(view)
    out = spark.sql("select *  from  countries").collect()
    #print type(out) #<class 'pyspark.sql.dataframe.DataFrame'> + collect() = list
    #print out

    #print("########################SELECT SINGLE COL######################")

    df1 = spark.sql("SELECT alpha_2,capital, population from  countries")
    c_list = df1.collect()
    #print c_list
    #spark.sql("select * from testtable")
#    for l in c_list:
        #print l['alpha_2'], l['capital'], l['population']
#        spark.sql("insert into  testtable values('india', 'd', 100)") 
    #spark.sql("select alpha_3, captital, population into testtable from  countries")
    #spark.sql("insert into  testtable(country, captital, population) select alpha_3, captital, population from  countries")
    #spark.sql("select * from testtable")

if __name__ == "__main__":
    if len(sys.argv) > 1:
	print sys.argv
        print("Wrong Usage: ")
        sys.exit(-1)

    #readCSV()
    createDB()
    createTB()
    populateTB()

    spark.stop()
