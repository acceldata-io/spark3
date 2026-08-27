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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, InSet}
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering

/**
 * Adds runtime partition filtering, e.g. dynamic partition pruning, to a [[FileScan]]. Filters
 * that arrive through [[filter]] are appended to the ones known at planning time, so that
 * `planInputPartitions` lists only the partitions that can still match.
 */
trait FileScanRuntimeFiltering extends FileScan with SupportsRuntimeV2Filtering {

  protected def staticPartitionFilters: Seq[Expression]

  private var dynamicPartitionFilters: Seq[Expression] = Seq.empty

  protected def dynamicPartitionFiltersSql: Seq[String] = dynamicPartitionFilters.map(_.sql)

  override def partitionFilters: Seq[Expression] =
    staticPartitionFilters ++ dynamicPartitionFilters

  override def filterAttributes(): Array[NamedReference] = {
    // The scan is already planned, so only partition columns kept in the read schema are usable.
    val scanFields = readSchema().fieldNames.toSet
    readPartitionSchema.fieldNames.filter(scanFields.contains).map(FieldReference.column)
  }

  override def filter(predicates: Array[Predicate]): Unit = {
    val resolver = sparkSession.sessionState.conf.resolver
    // Each call carries the complete set of runtime predicates, so replace rather than append.
    dynamicPartitionFilters = predicates.toSeq.flatMap {
      case p if p.name == "IN" && p.children.length > 1 =>
        val values = p.children.tail.collect { case l: LiteralValue[_] => l.value }
        p.children.head match {
          case f: FieldReference
              if f.fieldNames.length == 1 && values.length == p.children.length - 1 =>
            readPartitionSchema.find(pf => resolver(pf.name, f.fieldNames.head)).map { pf =>
              InSet(AttributeReference(pf.name, pf.dataType, pf.nullable)(), values.toSet)
            }
          case _ => None
        }
      case _ => None
    }
  }
}
