package com.aws.analytics.conf

case class Config(
                   env: String = "",
                   brokerList: String = "",
                   sourceTopic: String = "",
                   consumerGroup: String = "",
                   checkpointDir: String ="",
                   trigger:String = "300",
                   partitionNum: String = "16",
                   hudiBasePath:String ="",
                   tableType:String="COW",
                   syncDB:String ="",
                   syncTableName:String ="",
                   startPos:String="latest",
                   morCompact:String="true",
                   inlineMax:String="20",
                   hudiWriteOperation:String = "insert" ,
                   hudiKeyField:String = "",
                   hudiCombineField:String = "",
                   hudiPartition:String = "",
                   zookeeperUrl:String = "",
                   zookeeperPort:String = "2181",
                   hudiIntervel: Int = 30000,
                   hudiODSTable:String = ""
                 )

object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config]("spark ss "+programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env: dev or prod")
      opt[String]('b', "brokerList").optional().action((x, config) => config.copy(brokerList = x)).text("kafka broker list,sep comma")
      opt[String]('t', "sourceTopic").optional().action((x, config) => config.copy(sourceTopic = x)).text("kafka topic")
      opt[String]('p', "consumerGroup").optional().action((x, config) => config.copy(consumerGroup = x)).text("kafka consumer group")
      opt[String]('o', "startPos").optional().action((x, config) => config.copy(startPos = x)).text("kafka start pos latest or earliest,default latest")
      opt[String]('i', "trigger").optional().action((x, config) => config.copy(trigger = x)).text("default 300 second,streaming trigger interval")
      opt[String]('c', "checkpointDir").optional().action((x, config) => config.copy(checkpointDir = x)).text("hdfs dir which used to save checkpoint")
      opt[String]('p', "partitionNum").optional().action((x, config) => config.copy(partitionNum = x)).text("repartition num,default 16")
      opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
      opt[String]('g', "hudiBasePath").optional().action((x, config) => config.copy(hudiBasePath = x)).text("hudi event table hdfs base path")
      opt[String]('s', "syncDB").optional().action((x, config) => config.copy(syncDB = x)).text("hudi sync hive db")
      opt[String]('u', "syncTableName").optional().action((x, config) => config.copy(syncTableName = x)).text("hudi sync hive table name")
      opt[String]('w', "hudiWriteOperation").optional().action((x, config) => config.copy(hudiWriteOperation = x)).text("hudi write operation,default insert")
      opt[String]('z', "hudiKeyField").optional().action((x, config) => config.copy(hudiKeyField = x)).text("hudi key field, recordkey")
      opt[String]('k', "hudiCombineField").optional().action((x, config) => config.copy(hudiCombineField = x)).text("hudi combine field, precombine")
      opt[String]('q', "hudiPartition").optional().action((x, config) => config.copy(hudiPartition = x)).text("hudi partition column,default logday,hm")
      opt[String]('t', "morCompact").optional().action((x, config) => config.copy(morCompact = x)).text("mor inline compact,default:true")
      opt[String]('m', "inlineMax").optional().action((x, config) => config.copy(inlineMax = x)).text("inline max compact,default:20")

      programName match {
        case "MSK2Hudi" =>

        case "Aggregation" =>
          opt[String]('d', "hudiODSTable").optional().action((x, config) => config.copy(hudiODSTable = x)).text("hudi table in ODS layer")
          opt[Int]('l', "hudiIntervel").optional().action((x, config) => config.copy(hudiIntervel = x)).text("interval for hudi incremetail query")
      }
    }

    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        System.exit(-1)
        null
      }
    }

  }
}

