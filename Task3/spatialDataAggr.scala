// scalac -d spatialDataAggr.jar -cp .:/usr/local/spark/jars/*:/usr/local/stark/stark.jar spatialDataAggr.scala && spark-submit --master yarn --jars /usr/local/stark/stark.jar --class spatialData spatialDataAggr.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col

import dbis.stark._
import dbis.stark.spatial.SpatialRDD._
import dbis.stark.spatial.partitioner.{SpatialGridPartitioner, SpatialPartitioner}
import scala.collection.mutable.ListBuffer

import scala.collection.immutable.ListMap


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

//+---+---+-------+---+--------------------+--------------------+
//|_c0|_c1|    _c2|_c3|                 _c4|                 _c5|
//+---+---+-------+---+--------------------+--------------------+
//|Cid| ??|????   |???|Country Name        |Country Map MPolygon|

  def world_level(spark: SparkSession) {
	val sc = spark.sparkContext

	val world_level = sc.textFile("/data/world_level2.csv")
	//df.foreach(println)
	val world_level_clean = world_level.map(line => line.split(';')).filter((arr => arr.length==6))
	val countries = world_level_clean.map(arr => (STObject(arr(5)), (arr(0).toInt,arr(4))))
	
	val gdelt = sc.textFile("/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv").map(line => line.split('\t')).filter(arr => arr(46).length > 0 )
	val gdelt_event_points = gdelt.map(arr => (STObject("POINT ( " + arr(46) + " " + arr(47) + " )"), (arr(0).toInt, arr(51)) ))

	// dimensions=2, lat, long
	val gridPartitioner = new SpatialGridPartitioner(rdd=gdelt_event_points, partitionsPerDimension=10, pointsOnly=true, dimensions=2)
	val gdelt_partitioned = gdelt_event_points.partitionBy(gridPartitioner).liveIndex(order = 5)

	val cids = world_level_clean.map(arr => arr(0).toInt).collect()

	var count:Long = 0;
	var country_event_count = Map("name"->count)

	for (cid <- cids) {
	    var country = countries.filter(arr => arr._2._1 == cid).first() //.first() (STObject(arr(5))
	    count = gdelt_partitioned.containedby(country._1).count()
	    country_event_count += (country._2._2 -> count)
	}
	
	val sorted = ListMap(country_event_count.toSeq.sortWith(_._1 > _._1):_*)
	sorted.drop(3).foreach(println)
	
  } // world_level


} // spatialDataAggr

//[Error]

//[Stage 30:=====================================================>(104 + 1) / 105]18/06/22 23:57:07 ERROR YarnScheduler: Lost executor 6 on dbblade14.prakinf.tu-ilmenau.de: Container marked as failed: container_1528188921975_0472_01_000007 on host: dbblade14.prakinf.tu-ilmenau.de. Exit status: 143. Diagnostics: Container killed on request. Exit code is 143
//Container exited with a non-zero exit code 143
//Killed by external signal

//Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: ResultStage 31 (count at spatialDataAggr.scala:67) has failed the maximum allowable number of times: 4. Most recent failure reason: org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle 0
