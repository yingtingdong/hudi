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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.collection.JavaConverters.asScalaIteratorConverter

class DeleteRollbackInstantProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getBasePath(tableName)

    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(tablePath).build

    val rollbackTimeline = metaClient.getActiveTimeline.getRollbackTimeline
    var result = false
    rollbackTimeline.filterInflights().getInstants.iterator().asScala.foreach((instant: HoodieInstant) => {
      logWarning("Trying to remove rollback instant file: " + instant)
      HoodieActiveTimeline.deleteInstantFile(metaClient.getFs, metaClient.getMetaPath, instant)
    })
    rollbackTimeline.filterInflightsAndRequested().getInstants.iterator().asScala.foreach((instant: HoodieInstant) => {
      logWarning("Trying to remove rollback instant file: " + instant)
      HoodieActiveTimeline.deleteInstantFile(metaClient.getFs, metaClient.getMetaPath, instant)
    })
    result = true
    Seq(Row(result))
  }

  override def build: Procedure = new DeleteRollbackInstantProcedure()
}

object DeleteRollbackInstantProcedure {
  val NAME = "delete_rollback_instant"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new DeleteRollbackInstantProcedure()
  }
}
