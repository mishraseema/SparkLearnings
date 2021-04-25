package cma.sparklearning

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object HelloSpark extends Serializable{

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    logger.info("Start spark")

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    logger.info("Stop spark")
    spark.stop()

  }

  def getSparkConf: SparkConf = {
    val conf = new SparkConf
    
    val props = new Properties
    import scala.io.Source
    props.load(Source.fromFile("sparkconf").bufferedReader())

    import scala.collection.JavaConverters._
    props.asScala.foreach(kv => conf.set(kv._1, kv._2))

    conf

  }

}
