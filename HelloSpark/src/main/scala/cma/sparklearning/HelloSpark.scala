package cma.sparklearning

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HelloSpark extends Serializable{

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    logger.info("Start spark")

    val spark = SparkSession.builder()
      .appName("Hello Spark")
      .master("local[3]")
      .getOrCreate()

    logger.info("Stop spark")
    spark.stop()

  }


}
