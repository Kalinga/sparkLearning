// scalac -d spatialDataAggr.jar -cp .:/usr/local/spark/jars/*:/usr/local/stark/stark.jar spatialDataAggr.scala && spark-submit --master yarn --jars /usr/local/stark/stark.jar --class spatialData spatialDataAggr.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col

import dbis.stark._
import dbis.stark.spatial.SpatialRDD._
import dbis.stark.spatial.partitioner.{SpatialGridPartitioner, SpatialPartitioner}
import scala.collection.mutable.ListBuffer

// For implicit conversions like converting RDDs to DataFrames
//import org.apache.spark.implicits._

object spatialDataAggr {

  def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .appName("SparkScala SpatialData app")
          .config("spark.some.config.option", "some-value")
          .getOrCreate()

        println("spatialData Aggr!")
//        val datafs = spark.read.option("delimiter", "\t").csv("/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv").take(1000)
//        val datafs_cols_46_47 = datafs.select(col("_c46"), col("_c47"))
//        datafs_cols_46_47.show()

        world_level(spark)
  }

  def world_level(spark: SparkSession) {
	if (false) {
            val df = spark.read.option("delimiter", ";").csv("/data/world_level2.csv")
            df.printSchema()
            val df_cols_id_name_ploy = df.select(col("_c0"),col("_c1"), col("_c2"), col("_c3"), col("_c4"),col("_c5"))
            df_cols_id_name_ploy.show()
   	}

	val sc = spark.sparkContext
	val df = sc.textFile("/data/world_level2.csv")
	//df.foreach(println)
	val df_clean = df.map(line => line.split(';')).filter((arr => arr.length==6))

//+---+---+-------+---+--------------------+--------------------+
//|_c0|_c1|    _c2|_c3|                 _c4|                 _c5|
//+---+---+-------+---+--------------------+--------------------+
//|Cid| ??|????   |???|Country Name        |Country Map MPolygon|

	val countries = df_clean.map(arr => (STObject(arr(5)), (arr(0).toInt,arr(4))))

//	for (c <- countries) {
//	    println(c._2)
//       }

//	System.exit(0)
	val gdelt = sc.textFile("/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv").map(line => line.split('\t')).filter(arr => arr(46).length > 0 )

	val gdelt_event_points = gdelt.map(arr => (STObject("POINT ( " + arr(46) + " " + arr(47) + " )"), (arr(0).toInt, arr(51)) ))

	// dimensions=2, lat, long
	val gridPartitioner = new SpatialGridPartitioner(rdd=gdelt_event_points, partitionsPerDimension=10, pointsOnly=true, dimensions=2)
	val gdelt_partitioned = gdelt_event_points.partitionBy(gridPartitioner)

	val cids = df_clean.map(arr => arr(0).toInt).collect()
	for (cid <- cids) {
	    var country = countries.filter(arr => arr._2._1==cid).first()
	    println(country._2._2,gdelt_partitioned.containedby(country._1).count())
	}

	
	

	//val geom_having_line  = partionedCountries.intersects(STObject("LINESTRING ( 1.4143211 42.5378868, 14.3443989 55.1476253 )")).map(arr => (arr._2) )
	//println(geom_having_line.collect().mkString("\n"))

	println("------------------------------------------------------------------------")
	println()
        println()
	println()
	println("------------------------------------------------------------------------")

  }



}
