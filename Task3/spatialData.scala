// scalac -d spatialData.jar -cp .:/usr/local/spark/jars/* spatialData.scala
// spark-submit --master yarn --jars /usr/local/stark/stark.jar --class spatialData spatialData.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

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
        val datafs = spark.read.option("delimiter", "\t").csv("/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv")
        val datafs_cols_46_47 = datafs.select(col("_c46"), col("_c47"))
        datafs_cols_46_47.show()
        world_level(spark)
  }

  def world_level(spark: SparkSession) {
        val df = spark.read.option("delimiter", ";").csv("/data/world_level2.csv")
        df.printSchema()
        df.show()
        //val df_cols_id_name_ploy = df.select(col("id"), col("name"),col("poly"))
        //val df_cols_id_name_ploy = df.select(col("_c0"), col("_c4"),col("_c5"))
        val df_cols_id_name_ploy = df.select(col("_c0"))
        df_cols_id_name_ploy.show()
  }
}
