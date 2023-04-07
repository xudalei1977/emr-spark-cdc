package com.aws.analytics.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHelper {


  def getSparkSession(env: String) = {

    env match {
      case "prod" => {
        val conf = new SparkConf()
          .setAppName("emr-spark-cdc")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.hive.convertMetastoreParquet", "false")
          .set("spark.debug.maxToStringFields", "500")
          .set("spark.sql.hive.metastore.version", "2.3.9-amzn-2")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }

      case "dev" => {
        val conf = new SparkConf()
          .setAppName("emr-spark-cdc")
          .setMaster("local[2]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.hive.convertMetastoreParquet", "false")
          .set("spark.debug.maxToStringFields", "500")
          .set("spark.sql.hive.metastore.version", "2.3.9-amzn-2")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }

      case _ => {
        println("not match env, exits")
        System.exit(-1)
        null
      }
    }
  }

}
