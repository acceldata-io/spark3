/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.v2.orc

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.orc.OrcOptions
import org.apache.spark.sql.execution.datasources.v2.FileScanRuntimeFiltering
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class OrcScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedAggregate: Option[Aggregation] = None,
    pushedFilters: Array[Filter],
    staticPartitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty) extends FileScanRuntimeFiltering {

  override def columnarSupportMode(): Scan.ColumnarSupportMode = {
    val conf = sparkSession.sessionState.conf
    if (!conf.getConf(SQLConf.V2_FILE_COLUMNAR_SUPPORT_WITHOUT_INPUT_PARTITIONS_ENABLED)) {
      Scan.ColumnarSupportMode.PARTITION_DEFINED
    } else if (OrcPartitionReaderFactory.supportsColumnarReads(
        conf, readDataSchema, readPartitionSchema)) {
      Scan.ColumnarSupportMode.SUPPORTED
    } else {
      Scan.ColumnarSupportMode.UNSUPPORTED
    }
  }

  override def isSplitable(path: Path): Boolean = {
    // If aggregate is pushed down, only the file footer will be read once,
    // so file should not be split across multiple tasks.
    pushedAggregate.isEmpty
  }

  override def readSchema(): StructType = {
    // If aggregate is pushed down, schema has already been pruned in `OrcScanBuilder`
    // and no need to call super.readSchema()
    if (pushedAggregate.nonEmpty) {
      readDataSchema
    } else {
      super.readSchema()
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val memoryMode = if (sparkSession.sessionState.conf.offHeapColumnVectorEnabled) {
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    OrcPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters, pushedAggregate,
      new OrcOptions(options.asScala.toMap, sparkSession.sessionState.conf), memoryMode)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: OrcScan =>
      val pushedDownAggEqual = if (pushedAggregate.nonEmpty && o.pushedAggregate.nonEmpty) {
        AggregatePushDownUtils.equivalentAggregations(pushedAggregate.get, o.pushedAggregate.get)
      } else {
        pushedAggregate.isEmpty && o.pushedAggregate.isEmpty
      }
      super.equals(o) && dataSchema == o.dataSchema && options == o.options &&
        equivalentFilters(pushedFilters, o.pushedFilters) && pushedDownAggEqual &&
        dynamicPartitionFiltersSql == o.dynamicPartitionFiltersSql
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions),
      seqToString(pushedAggregate.get.groupByExpressions))
  } else {
    ("[]", "[]")
  }

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters)) ++
      Map("PushedAggregation" -> pushedAggregationsStr) ++
      Map("PushedGroupBy" -> pushedGroupByStr)
  }
}
