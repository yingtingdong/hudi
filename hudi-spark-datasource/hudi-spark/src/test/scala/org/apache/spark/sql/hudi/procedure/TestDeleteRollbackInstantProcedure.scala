/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant}
import org.apache.spark.sql.catalyst.expressions.Log

import scala.jdk.CollectionConverters.asScalaIteratorConverter

class TestDeleteRollbackInstantProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call delete_rollback_instant Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
     """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")

      // 3 commits are left before rollback
      var commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3) {
        commits.length
      }

      // Call rollback_to_instant Procedure with Named Arguments
      var instant_time = commits(0).get(0).toString
      checkAnswer(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")(Seq(true))
      // Call rollback_to_instant Procedure with Positional Arguments
      instant_time = commits(1).get(0).toString
      checkAnswer(s"""call rollback_to_instant('$tableName', '$instant_time')""")(Seq(true))

      // 1 commits are left after rollback
      commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(1) {
        commits.length
      }

      // collect rollbacks for table
      val rollbacks = spark.sql(s"""call show_rollbacks(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        rollbacks.length
      }

      // make rollback instant to rollback inflight instant
      val metaClient = HoodieTableMetaClient.builder.setConf(spark.sparkContext.hadoopConfiguration)
        .setBasePath(s"${tmp.getCanonicalPath}/$tableName").build
      val rollbackTimeline = metaClient.getActiveTimeline.getRollbackTimeline
      rollbackTimeline.getInstants.iterator().asScala.foreach((instant: HoodieInstant) => {
        HoodieActiveTimeline.deleteInstantFile(metaClient.getFs, metaClient.getMetaPath, instant)
      })

      checkAnswer(s"""call delete_rollback_instant(table => '$tableName')""")(Seq(true))
    }
  }
}
