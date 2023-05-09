package com.aws

import org.apache.hudi.org.apache.hadoop.hbase.security.access.AuthResult.Params
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.aws.analytics.conf.Config
import java.time.LocalDateTime
import scala.collection.mutable

package object analytics {

  def writeHudiTable(batchDF: DataFrame, params: Config): Unit = {

    val hasZookeeperLock = true
    val newsDF = batchDF.filter(_ != null)

    val options = mutable.Map(
      "hoodie.table.name" -> params.syncTableName,
      "hoodie.datasource.write.table.name" -> params.syncTableName,
      "hoodie.datasource.write.operation" -> params.hudiWriteOperation,
      "hoodie.datasource.write.recordkey.field" -> params.hudiKeyField,
      "hoodie.datasource.write.precombine.field" -> params.hudiCombineField,
      "hoodie.upsert.shuffle.parallelism" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "2",
      "hoodie.datasource.hive_sync.mode" -> "hms",
      "hoodie.datasource.hive_sync.auto_create_database" -> "true",
      "hoodie.datasource.hive_sync.enable" -> "true",
      "hoodie.datasource.hive_sync.database" -> params.syncDB,
      "hoodie.datasource.hive_sync.table" -> params.syncTableName,
      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.DefaultHoodieRecordPayload"
    )

//    if (hasZookeeperLock) {
//      options ++= mutable.Map(
//        "hoodie.write.concurrency.mode" -> "optimistic_concurrency_control",
//        "hoodie.cleaner.policy.failed.writes" -> "LAZY",
//        "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider",
//        "hoodie.write.lock.zookeeper.url" -> zookeeperUrl,
//        "hoodie.write.lock.zookeeper.port" -> "2181",
//        "hoodie.write.lock.zookeeper.lock_key" -> tableName,
//        "hoodie.write.lock.zookeeper.base_path" -> "/hudi/write_lock")
//    }

    if(params.tableType.toUpperCase() == "MOR")
      options ++= mutable.Map(
        "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
        "hoodie.compact.inline" -> params.morCompact,
        "hoodie.compact.inline.max.delta.commits" -> params.inlineMax)
    else {
      options ++= mutable.Map(
        "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE"
      )
    }

    if (params.hudiPartition != "") {
      options ++= mutable.Map(
        "hoodie.datasource.write.partitionpath.field" -> params.hudiPartition,
//        "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
//        "hoodie.datasource.write.hive_style_partitioning" -> "true",
        "hoodie.datasource.hive_sync.partition_fields" -> params.hudiPartition,
        "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor"
      )

    } else {
      options ++= mutable.Map(
        "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.NonPartitionedExtractor"
      )
    }

    print(" ********** option : " + options.mkString)

//    val hudiOptions = mutable.Map(
//      "hoodie.table.name" -> params.syncTableName,
//      "hoodie.datasource.write.table.name" -> params.syncTableName,
//      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
//      "hoodie.datasource.write.operation" -> "upsert",
//      "hoodie.datasource.write.recordkey.field" -> "id",
//      "hoodie.datasource.write.partitionpath.field" -> "age",
//      "hoodie.datasource.write.precombine.field" -> "createTS",
//      "hoodie.upsert.shuffle.parallelism" -> "2",
//      "hoodie.insert.shuffle.parallelism" -> "2",
//      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
//      "hoodie.delete.shuffle.parallelism" -> "2",
//      "hoodie.datasource.hive_sync.mode" -> "hms",
//      "hoodie.datasource.hive_sync.auto_create_database" -> "true",
//      "hoodie.datasource.hive_sync.database" -> "dev",
//      "hoodie.datasource.hive_sync.table" -> params.syncTableName,
//      "hoodie.datasource.hive_sync.partition_fields" -> "age",
//      "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor",
//      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
//      "hoodie.datasource.hive_sync.enable" -> "true"
//    )

    if (!newsDF.isEmpty) {
      newsDF.persist()
      newsDF.show()
      println(LocalDateTime.now() + " === start writing table")

      try {
        newsDF.write.format("hudi").options(options)
          .mode(SaveMode.Overwrite)
          .save(s"${params.hudiBasePath}/${params.syncDB}/${params.syncTableName}")
      } catch {
        case e: Exception => e.printStackTrace
      }

      newsDF.unpersist()
      println(LocalDateTime.now() + " === finish")
    }
  }

}

