package cma.sparklearning

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HelloSpark extends Serializable{

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    logger.info("Start spark")

    //refer https://spark.apache.org/docs/latest/configuration.html for config proprties
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "Hello Spark")
    sparkConf.set("spark.master", "local[3]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    logger.info("Stop spark")
    spark.stop()

  }


}
