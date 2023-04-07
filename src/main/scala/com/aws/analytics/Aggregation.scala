package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory
import org.apache.hudi.DataSourceReadOptions._

import java.util.Date
import java.text.SimpleDateFormat


object Aggregation {

  private val log = LoggerFactory.getLogger("Aggregation")
  private val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS")

  def main(args: Array[String]): Unit = {

    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)
    val params = Config.parseConfig(Aggregation, args)
    println("************* params : " + params.toString)

    val spark = SparkHelper.getSparkSession(params.env)
    import spark.implicits._

    //process the initial aggregation table, you can put this step out of this function.
    spark.read.format("hudi")
          .load(s"${params.hudiBasePath}/${params.syncDB}/${params.hudiODSTable}")
          .createOrReplaceTempView("init_detail")

    spark.sql("select cardDate, status, count(id) as order_sum, sum(cast(money as double)) as money_sum from init_detail group by cardDate, status")
          .createOrReplaceTempView("init_agg")

    val initAggDF = spark.sql("select cardDate, status, cast(unix_timestamp() as string) as ts, order_sum, money_sum from init_agg").toDF()

    writeHudiTable(initAggDF, params)

    //get the last commit time.
    spark.read.format("hudi").
          load(s"${params.hudiBasePath}/${params.syncDB}/${params.syncTableName}").
          createOrReplaceTempView("taxi_order_agg")

    val commits = spark.sql("select max(_hoodie_commit_time) as commitTime from taxi_order_agg").
      map(row => row.getString(0)).collect()

    var beginTime = commits(0) // commit time we are interested in
    var endTime: String = ""

    while(true){
      Thread.sleep(params.hudiIntervel)

      endTime = DATE_FORMAT.format(new Date())

      spark.read.format("hudi").
            option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL).
            option(BEGIN_INSTANTTIME.key(), beginTime).
            option(END_INSTANTTIME.key(), endTime).
            load(s"${params.hudiBasePath}/${params.syncDB}/${params.hudiODSTable}").
            createOrReplaceTempView("incre_detail")

      spark.sql("select cardDate, status, count(id) as order_sum, sum(cast(money as double)) as money_sum from incre_detail group by cardDate, status").
        createOrReplaceTempView("incre_agg")

      spark.read.format("hudi").
        load(s"${params.hudiBasePath}/${params.syncDB}/${params.syncTableName}").
        createOrReplaceTempView("taxi_order_agg")

      val increAggDF = spark.sql(
        """select i.cardDate, i.status, cast(unix_timestamp() as string) as ts,
          | (i.order_sum + s.order_sum) as order_sum, (i.money_sum + s.money_sum) as money_sum
          | from incre_agg i join taxi_order_agg s on i.cardDate = s.cardDate and i.status = s.status""".stripMargin)

      writeHudiTable(increAggDF, params)
      beginTime = endTime
    }
  }
}

//spark-submit --master yarn --deploy-mode client --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 5 \
//  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.12.1,org.apache.spark:spark-avro_2.12:3.3.0,org.apache.hudi:hudi-client:0.12.1,org.apache.hudi:hudi-hadoop-mr-bundle:0.12.1 \
//  --jars /usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://airflow-us-east-1-551831295244/jar/commons-pool2-2.11.1.jar,s3://airflow-us-east-1-551831295244/jar/aws-msk-iam-auth-1.1.1-all.jar,s3://airflow-us-east-1-551831295244/jar/scopt_2.12-4.0.0-RC2.jar \
//  --class com.aws.analytics.Aggregation s3://airflow-us-east-1-551831295244/jar/emr-spark-cdc-1.0-SNAPSHOT.jar \
//  -e prod \
//  -y cow -g s3://airflow-us-east-1-551831295244 -s dev -u taxi_order_agg \
//  -w upsert -z cardDate,status -k ts -d taxi_order -l 60000
