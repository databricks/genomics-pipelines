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

import java.io.FileNotFoundException

import io.projectglow.common.logging.HlsUsageLogging
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * Utility functions to help read and write from Delta.
 *
 * Note: This object was originally introduced as a compatibility layer between parquet and Delta.
 * If you know you're working with Delta 100% of the time, which is typically the case now that
 * we build and test only against DBR, prefer to interface with Delta directly.
 */
object DeltaHelper extends HlsUsageLogging {

  // Used if no codec is set via spark.sql.parquet.compression.codec
  // Snappy decompresses efficiently and is default in Spark.
  private val defaultCompressionCodec = CompressionCodecName.SNAPPY.toString.toLowerCase()

  // Used if no block size is set via parquet.block.size in Hadoop Conf
  // lower block sizes are better for selective queries.
  private val defaultBlockSize = 10 * 1024 * 1024

  /**
   * Save this dataset, optionally partitioning by given column.
   * Existing partitions are overwritten with the new data, while new partitions are appended.
   *
   * @param dataset The dataset to save
   * @param path The path to save this dataset
   * @param partitionBy Partitioning Column(s) if applicable as well as the partition values
   *                    touched by this write.
   * @param mode SaveMode: can be one of (overwrite, append, ignore, errorIfExists)
   */
  def save(
      dataset: Dataset[_],
      path: String,
      partitionBy: Option[(String, Set[_])] = None,
      mode: String = "append"): Unit = {
    val writer = partitionBy match {
      case None => newWriter(dataset, mode)
      case Some((partitionCol, existingValues)) =>
        require(existingValues.nonEmpty, "Must provide partition values to overwrite")
        require(!existingValues.exists(_ == null), "Partition values cannot be null")
        // string columns need quotes to be correctly resolved
        // this works in general since other columns will be cast into the wider type
        val stringifiedValues = existingValues.map(v => s"'$v'")
        val replaceWhere = s"$partitionCol in (${stringifiedValues.mkString(",")})"
        newWriter(dataset)
          .partitionBy(partitionCol)
          .option("replaceWhere", replaceWhere)
    }

    writer.save(path)
  }

  /**
   * Checks if a Delta or Parquet table exists. If the partition is provided, we check for the
   * existence of only that partition. If not, we check for any data in the table.
   */
  def tableExists[T](
      session: SparkSession,
      basePath: String,
      partition: Option[(String, T)]): Boolean = {
    val hPath = new Path(basePath)
    val fs = hPath.getFileSystem(session.sparkContext.hadoopConfiguration)
    val pathExists = fs.exists(hPath)

    if (pathExists) {
      if (!fs.listStatus(hPath).isEmpty) {
        val baseDf = session.read.format("delta").load(basePath)
        val df = partition match {
          case None => baseDf
          case Some((partCol, partVal)) =>
            baseDf.where(col(partCol) === partVal)
        }
        !df.isEmpty
      } else {
        false
      }
    } else {
      false
    }
  }

  private def newWriter[T](ds: Dataset[T], mode: String = "overwrite"): DataFrameWriter[T] = {

    val session = ds.sparkSession
    val codecKey = "spark.sql.parquet.compression.codec"
    val codec = session.conf.get(codecKey, defaultCompressionCodec)
    val blockSizeKey = "parquet.block.size"
    val hc = session.sparkContext.hadoopConfiguration
    val blockSize = hc.get(blockSizeKey, defaultBlockSize.toString)

    ds.write
      .mode(mode)
      .format("delta")
      .option(codecKey, codec)
      .option(blockSizeKey, blockSize)
  }
}

object DirectoryHelper {

  /**
   * Checks to see if a directory was written to with the directory atomic commit protocol.
   *
   * Note that since this protocol is only available on actual Databricks clusters, this function
   * can't be fully tested in unit tests.
   */
  def directoryWasCommitted(session: SparkSession, path: String): Boolean = {
    val hPath = new Path(path)
    val fs = hPath.getFileSystem(session.sparkContext.hadoopConfiguration)

    // List files instead of using `exists` directly to avoid possible issues with S3 negative
    // caching.
    try {
      fs.listStatus(hPath).exists(_.getPath.getName == DIRECTORY_COMMIT_SUCCESS_FILE)
    } catch {
      case _: FileNotFoundException => false
    }
  }

  val DIRECTORY_COMMIT_SUCCESS_FILE = "_SUCCESS"
}
