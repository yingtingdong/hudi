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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.common.fs.FSUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class DeleteFsFileProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "path", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("file", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val path = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val delFilePath: Path = new Path(path)

    val fs = FSUtils.getFs(delFilePath, jsc.hadoopConfiguration())
    val status: Array[FileStatus] = fs.globStatus(delFilePath)
    val rows: java.util.List[Row] = new java.util.ArrayList[Row]()

    if (status.nonEmpty) {
      for (i <- status.indices) {
        var result = false

        try {
          result = fs.delete(status(i).getPath, true)
        } catch {
          case e: Exception => System.err.println(s"delete ${status(i).getPath} failed due to", e)
        }

        rows.add(Row(result, status(i).getPath.toString))
      }
    }

    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new DeleteFsFileProcedure()
}

object DeleteFsFileProcedure {
  val NAME = "delete_fs_file"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new DeleteFsFileProcedure()
  }
}



