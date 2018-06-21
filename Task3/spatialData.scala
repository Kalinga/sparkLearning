// scalac -d spatialData.jar -cp .:/usr/local/spark/jars/*:/usr/local/stark/stark.jar spatialData.scala
// spark-submit --master yarn --jars /usr/local/stark/stark.jar --class spatialData spatialData.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col

import dbis.stark._
import dbis.stark.spatial.SpatialRDD._
import scala.collection.mutable.ListBuffer

// For implicit conversions like converting RDDs to DataFrames
//import org.apache.spark.implicits._

object spatialData {

  def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .appName("SparkScala SpatialData app")
          .config("spark.some.config.option", "some-value")
          .getOrCreate()

        println("spatialData Exploration!")
//        val datafs = spark.read.option("delimiter", "\t").csv("/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv")
//        val datafs_cols_46_47 = datafs.select(col("_c46"), col("_c47"))
//        datafs_cols_46_47.show()

        world_level(spark)
  }

  def world_level(spark: SparkSession) {
	if (false) {
            val df = spark.read.option("delimiter", ";").csv("/data/world_level2.csv")
            df.printSchema()
//root                                                                            
// |-- _c0: string (nullable = true)
// |-- _c1: string (nullable = true)
// |-- _c2: string (nullable = true)
// |-- _c3: string (nullable = true)
// |-- _c4: string (nullable = true)
// |-- _c5: string (nullable = true)

//+---+---+-------+---+--------------------+--------------------+
//|_c0|_c1|    _c2|_c3|                 _c4|                 _c5|
//+---+---+-------+---+--------------------+--------------------+
//| 92|  2| 295480|  0|            Portugal|MULTIPOLYGON(((-1...|
//|108|  2| 364110|  0|          Azərbaycan|MULTIPOLYGON(((46...|
//|114|  2| 550727|  0|             Grenada|MULTIPOLYGON(((-6...|
//|122|  2|1703814|  0|                null|MULTIPOLYGON(((34...|

            //df.show()
            val df_cols_id_name_ploy = df.select(col("_c0"),col("_c1"), col("_c2"), col("_c3"), col("_c4"),col("_c5"))
            df_cols_id_name_ploy.show()
   	}

	val sc = spark.sparkContext
	val df = sc.textFile("/data/world_level2.csv")
	val df_clean = df.map(line => line.split(';')).filter((arr => arr.length==6))
	val countries = df_clean.map(arr => (STObject(arr(5)), (arr(4)))) 
	
	// find all geometries that contain the given point 
	val geom_having_point = countries.contains(STObject("POINT(1.4143211 42.5378868)"))
	println(geom_having_point.map(arr=>arr._2).collect().mkString("\n")) // No Output!

	val geom_having_line  = countries.intersects(STObject("LINESTRING ( 1.4143211 42.5378868, 14.3443989 55.1476253 )")).map(arr => (arr._2) )
	println(geom_having_line.collect().mkString("\n"))
//--------------------------------------
//Andorra                                                                         
//Danmark
//España
//Deutschland
//--------------------------------------

val A = Array("9.363038", "56.079044")
val B = Array("-3.385057", "48.347321")
//C (15.641353, 40.924231)
//D (31.569410, 38.767526)
//E (33.466313, 49.500775)
	val coordinateMap  = Map('A' -> "9.363038 56.079044", 'B' -> "-3.385057 48.347321",'C'->"15.641353 40.924231", 'D'->"31.569410 38.767526",'E'->"33.466313 49.500775")

	// Consider the number of countries on the path as distance
	// A=>B
  
        //val A_B  = countries.intersects(STObject("LINESTRING (" + A.mkString(" ") + "," + B.mkString(" ") + ")")).map(arr => (arr._2) )
	//println(A_B.collect().mkString("\n"))
	//val AB  = countries.intersects(STObject("LINESTRING (" + coordinateMap("A") + "," + coordinateMap("B")+ ")")).map(arr => (arr._2) )
        //println(AB.collect().mkString("\n"))
//Danmark                                                                         
//Jersey
//United Kingdom
//Nederland
	
	val points = "ABCDE"
	val pList = points.toList
	//println(pList)
	val paths = pList.permutations.toList
	//println(paths)
	var count = 0
	var minCount:Long = 0
	var optPath = ""
	var _countries_list = ""
	for (path <- paths) {
		var _path = count + ":" + path.mkString(" -> ")
		//println(_path)
		count += 1
	
		var multiLine = new ListBuffer[String]()
		for (point <- path) {
		        multiLine += coordinateMap(point)
    		}
		
		var _countries = countries.intersects(STObject("LINESTRING ("+multiLine.toList.mkString(",")+")")).map(arr => (arr._2) )
		var _countries_count = _countries.count()
		var _cList = ( _countries.collect().mkString(","))
		println(_cList)
		println(_countries_count)
		if(0 == minCount) {
			minCount = _countries_count
			_countries_list = _cList
		}
		else if (_countries_count < minCount) {
			minCount = _countries_count
			optPath = _path
			_countries_list = _cList
		}
		
	}

	println("------------------------------------------------------------------------")
	println(optPath)
        println(minCount)
	println(_countries_list)
	println("------------------------------------------------------------------------")

  }



}
