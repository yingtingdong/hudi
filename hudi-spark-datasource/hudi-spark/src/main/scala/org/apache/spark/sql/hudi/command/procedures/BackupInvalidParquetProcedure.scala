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

import org.apache.hadoop.fs.Path
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.SerializableConfiguration
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.util.BaseFileUtils
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.collection.JavaConversions.asScalaBuffer
import scala.jdk.CollectionConverters.seqAsJavaListConverter

class BackupInvalidParquetProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "path", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "is_partition", DataTypes.BooleanType, false),
    ProcedureParameter.optional(2, "parallelism", DataTypes.IntegerType, 100)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("backup_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("invalid_parquet_size", DataTypes.LongType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val srcPath = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val isPartition = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Boolean]
    val parallelism = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]

    val backupPath = new Path(srcPath, ".backup").toString
    val fs = FSUtils.getFs(backupPath, jsc.hadoopConfiguration())
    fs.mkdirs(new Path(backupPath))

    val serHadoopConf = new SerializableConfiguration(jsc.hadoopConfiguration())
    val partitionPaths: java.util.List[Path] = if (isPartition) {
      List(new Path(srcPath)).asJava
    } else {
      FSUtils.getAllPartitionPaths(new HoodieSparkEngineContext(jsc), srcPath, false, false)
        .map(part => FSUtils.getPartitionPath(srcPath, part))
        .toList.asJava
    }
    val javaRdd: JavaRDD[Path] = jsc.parallelize(partitionPaths, partitionPaths.size())
    val invalidParquetCount = javaRdd.rdd.map(part => {
      val fs = FSUtils.getFs(new Path(srcPath), serHadoopConf.get())
      FSUtils.getAllDataFilesInPartition(fs, part)
    }).flatMap(_.toList).repartition(parallelism)
      .filter(status => {
        val filePath = status.getPath
        var isInvalid = false
        if (filePath.toString.endsWith(".parquet")) {
          try {
            // check footer
            ParquetFileReader.readFooter(serHadoopConf.get(), filePath, SKIP_ROW_GROUPS).getFileMetaData

            // check row group
            BaseFileUtils.getInstance(HoodieFileFormat.PARQUET).readAvroRecords(serHadoopConf.get(), filePath)
          } catch {
            case e: Exception =>
              isInvalid = true
              filePath.getFileSystem(serHadoopConf.get()).rename(filePath, new Path(backupPath, filePath.getName))
          }
        }
        isInvalid
      })
      .count()
    Seq(Row(backupPath, invalidParquetCount))
  }

  override def build = new BackupInvalidParquetProcedure()
}

object BackupInvalidParquetProcedure {
  val NAME = "backup_invalid_parquet"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new BackupInvalidParquetProcedure()
  }
}



