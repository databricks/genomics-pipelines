/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.projectglow.pipelines.common

import java.io.{File, FileFilter}
import java.nio.file.Files

import io.projectglow.pipelines.PipelineBaseTest
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class DeltaHelperSuite extends PipelineBaseTest {

  override protected def sparkConf: SparkConf = {
    // Since Spark 2.3, setting partition mode overwrite to dynamic
    // allows Spark to overwrite only select partitions instead of rewriting
    // the entire table. We use this feature to incrementally write new partitions.
    super.sparkConf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  }

  test("save: append without partitions") {

    val outputDir = Files.createTempDirectory("txn")
    val path = outputDir.resolve("0").toString
    val d1 = spark.range(0, 5)
    val d2 = spark.range(5, 10)

    DeltaHelper.save(d1, path)
    DeltaHelper.save(d2, path)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val results = spark.read.format("delta").load(path).as[Long]
    assert(results.count() == 10)
  }

  test("save: append partitions") {
    val outputDir = Files.createTempDirectory("txn")
    val path = outputDir.resolve("1").toString

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val d1 = spark.range(0, 5).withColumn("value", $"id" + 1)

    DeltaHelper.save(d1, path, Some(("id", (0 until 5).toSet)))

    // this should create 5 partitions
    val partitions = new File(path).listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.isDirectory && !pathname.toString.endsWith("_delta_log")
      }
    })

    assert(partitions.length == 5)
  }

  test("save: overwrite partitions") {
    val outputDir = Files.createTempDirectory("txn")
    val path = outputDir.resolve("1").toString

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val d1 = spark.range(0, 5).withColumn("value", $"id" + 1)
    val d2 = spark.range(0, 3).withColumn("value", $"id" + 2)

    DeltaHelper.save(d1, path, Some(("id", (0 until 5).toSet)))
    DeltaHelper.save(d2, path, Some(("id", (0 until 3).toSet)))

    // this should create 5 partitions
    val partitions = new File(path).listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.isDirectory && !pathname.toString.endsWith("_delta_log")
      }
    })

    assert(partitions.length == 5)

    val actual = spark
      .read
      .format("delta")
      .load(path)
      .filter($"id" isin (2, 4))
      .select($"value")
      .map {
        case Row(v: Long) => v
      }
      .collect()
      .sorted
    val expected = Array(4L, 5L)

    assert(expected sameElements actual)
  }

  case class Datum(key: String, value: String)
  test("check if partition exists (whole table)") {
    val path = Files.createTempDirectory("tables")
    val tablePath = path.resolve("table").toString
    assert(!DeltaHelper.tableExists(spark, tablePath, None))
    spark.createDataFrame(Seq(Datum("a", "b"))).write.format("delta").save(tablePath)
    assert(DeltaHelper.tableExists(spark, tablePath, None))
  }

  test("check if partition exists (specific partition)") {
    val path = Files.createTempDirectory("tables")
    val tablePath = path.resolve("table").toString
    val partition = ("key", "monkey")
    assert(!DeltaHelper.tableExists(spark, tablePath, Some(partition)))
    spark
      .createDataFrame(Seq(Datum("a", "b")))
      .write
      .partitionBy("key")
      .format("delta")
      .save(tablePath)
    assert(!DeltaHelper.tableExists(spark, tablePath, Some(partition)))
    // Note: We cannot check the behavior where a partition does exist in a unit test because
    // the per-directory success files are only written out by the Databricks directory
    // atomic commit protocol
  }

  test("No table if no such directory") {
    val path = Files.createTempDirectory("table")
    Files.delete(path)
    assert(!DeltaHelper.tableExists(spark, path.toString, None))
  }

  test("No table if empty directory") {
    val path = Files.createTempDirectory("table")
    assert(!DeltaHelper.tableExists(spark, path.toString, None))
  }
}
