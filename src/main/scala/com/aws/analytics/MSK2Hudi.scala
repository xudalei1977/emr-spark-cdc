package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory


object MSK2Hudi {

  private val log = LoggerFactory.getLogger("MSK2Hudi")

  def main(args: Array[String]): Unit = {

    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)
    val params = Config.parseConfig(MSK2Hudi, args)

    println("************* params : " + params.toString)
    implicit val spark = SparkHelper.getSparkSession(params.env)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", params.brokerList)
      .option("subscribe", params.sourceTopic)
//      .option("kafka.security.protocol", "SASL_SSL")
//      .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
//      .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
//      .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
      .option("startingOffsets", params.startPos)
      .option("failOnDataLoss", false)
      .load()
      .repartition(Integer.valueOf(params.partitionNum))

    val query = df.writeStream.
      queryName("MSK2Hudi").
      foreachBatch { (batchDF: DataFrame, _: Long) =>
        if(batchDF != null && (!batchDF.isEmpty)) {
          batchDF.persist()
          val jsonDF = batchDF.withColumn("json", col("value").cast(StringType)).select("json")

          import spark.implicits._
          val json_schema = spark.read.json(jsonDF.select("json").as[String]).schema
          println("json_schema : " + json_schema)

          val df = jsonDF.select(from_json(col("json"), json_schema) as "data")
                .select("data.*")
                .drop("__op")
                .drop("__source_connector")
                .drop("__source_db")
                .drop("__source_table")
                .drop("__source_file")
                .drop("__source_pos")
                .drop("__source_ts_ms")
                .drop("__deleted")
                .where("data.id is not null")

          writeHudiTable(df, params)
        }
      }
      .option("checkpointLocation", params.checkpointDir)
      .trigger(Trigger.ProcessingTime(params.trigger + " seconds"))
      .start

    query.awaitTermination()
  }

}

//spark-submit --master yarn --deploy-mode client --driver-cores 1 --driver-memory 4G --executor-cores 2 --executor-memory 8G --num-executors 5 \
//  --jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/connector/lib/spark-avro.jar,/usr/lib/hudi/cli/lib/hudi-client-common-0.15.0-amzn-2.jar,/usr/lib/hudi/hudi-hadoop-mr-bundle.jar,/usr/lib/spark/connector/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/connector/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.8.0.jar,s3://airflow-us-east-1-551831295244/jar/commons-pool2-2.11.1.jar,s3://airflow-us-east-1-551831295244/jar/aws-msk-iam-auth-1.1.1-all.jar,s3://airflow-us-east-1-551831295244/jar/scopt_2.12-4.0.0-RC2.jar \
//  --class com.aws.analytics.MSK2Hudi s3://airflow-us-east-1-551831295244/jar/emr-spark-cdc-1.0-SNAPSHOT.jar \
//  -e prod \
//  -b b-1.emrworkshopmsk.prs0lr.c17.kafka.us-east-1.amazonaws.com:9092,b-2.emrworkshopmsk.prs0lr.c17.kafka.us-east-1.amazonaws.com:9092,b-3.emrworkshopmsk.prs0lr.c17.kafka.us-east-1.amazonaws.com:9092 \
//  -t mysql.dev.taxi_order -p msk-consumer-group-01 -o earliest -c s3://airflow-us-east-1-551831295244/checkpoint-1/ -i 60 \
//  -y cow -g s3://airflow-us-east-1-551831295244 -s dev -u taxi_order \
//  -w upsert -z id -k createTS -q age
//
//spark-submit --master yarn --deploy-mode cluster --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 4 \
//  --class com.aws.analytics.MSK2Hudi ./emr-spark-cdc-1.0-SNAPSHOT-jar-with-dependencies.jar  \
//  -e prod \
//  -b b-1.emrworkshopmsk.prs0lr.c17.kafka.us-east-1.amazonaws.com:9092,b-2.emrworkshopmsk.prs0lr.c17.kafka.us-east-1.amazonaws.com:9092,b-3.emrworkshopmsk.prs0lr.c17.kafka.us-east-1.amazonaws.com:9092 \
//  -t mysql.dev.taxi_order -p msk-consumer-group-01 -o latest -c /user/hadoop/checkpoint/ -i 60 \
//  -y cow -g s3://airflow-us-east-1-551831295244 -s dev -u taxi_order \
//  -w upsert -z id -k createTS -q age
//
//insert into taxi_order values
//(13,5,57,13426827401,'yeguiying@yahoo.com','192.178.148.204','11/28','36217498828449','山西省莹县牧野陶街h座 796615','甘肃省深圳县大兴宁德路E座 962065','1-357-12962-9','a','白荣','440205195905270468','徐秀荣','20201027',99.2,62.72474,74.4654936,62.191423,10.5832138,40.9,1680696444,1680696444);
