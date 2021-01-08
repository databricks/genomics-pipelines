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

package io.projectglow.pipelines.dnaseq

import scala.util.control.NonFatal
import scala.reflect.runtime.universe._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

import io.projectglow.common.GlowLogging

/**
 * Sample Manifest Entry Definition.
 * There is exactly one manifest entry per (sample, read_group)
 *
 * @param file_path  The relative path of the sample
 * @param sample_id  The sample id
 * @param paired_end The paired end this manifest entry corresponds to (1|2|empty)
 */
case class ManifestEntry(
    file_path: String,
    sample_id: String,
    read_group_id: String,
    paired_end: String)

trait HasSampleInfo {

  /**
   * Short (fits on one line) description of the sample or group of samples contained in this
   * piece of metadata.
   */
  def infoTag: String
}

case class SampleMetadata(sample_id: String, read_groups: Seq[ReadGroupMetadata])
    extends HasSampleInfo {

  override def infoTag: String = sample_id
}

/**
 * Metadata corresponding to each read group.
 *
 * @param read_group_id
 * @param input (globbed) path to primary reads for a given read group
 * @param secondInput (globbed) path to reads for the second of pair if paired FASTQ
 */
case class ReadGroupMetadata(
    read_group_id: String,
    input: String,
    secondInput: Option[String] = None)

/**
 * Manifest read from either a path or a blob containing:
 * - source: how the manifest was read
 * - contents: a DataFrame containing the rows in the manifest
 * - resolve(path): a function to resolve the paths in the manifest
 * - validateColumns(expectedColumns): a function to check if the contents match the expected format
 */
sealed trait Manifest {
  def source: String
  def contents: DataFrame
  def resolve(entryPath: String): String
  def validateColumns(expectedColumns: Seq[String]): Unit = {
    if (contents.columns.toSeq.sorted != expectedColumns.sorted) {
      throw new IllegalArgumentException(
        s"""
           |s"Manifest read from $source '$manifest' does not have the expected columns."
           |Expected columns: ${expectedColumns.mkString(",")}
           |Actual columns: ${contents.columns.mkString(",")}
          """.stripMargin
      )
    }
  }
}

/**
 * Manifest read from a path
 */
case class ManifestPath(contents: DataFrame, basePath: String) extends Manifest {
  override def source: String = "path"
  // Resolves the absolute location of this entry relative to the manifest's path
  override def resolve(entryPath: String): String = {
    val parentPath = new Path(basePath).getParent
    new Path(parentPath, new Path(entryPath)).toString
  }
}

/**
 * Manifest read from a blob
 */
case class ManifestBlob(contents: DataFrame) extends Manifest {
  override def source: String = "blob"
  // Returns entry paths without resolution
  override def resolve(entryPath: String): String = {
    new Path(entryPath).toString
  }
}

object Manifest extends GlowLogging {

  def pathExists(manifest: String)(implicit sqlContext: SQLContext): Boolean = {
    try {
      val manifestPath = new org.apache.hadoop.fs.Path(manifest)
      val fs = manifestPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      fs.exists(manifestPath)
    } catch {
      case NonFatal(e) =>
        logger.warn(e.getMessage)
        false
    }
  }

  def loadCsvManifest(manifestBlobOrPath: String, expectedColumns: Seq[String])(
      implicit sqlContext: SQLContext): Manifest = {
    import sqlContext.implicits._
    val manifest = if (pathExists(manifestBlobOrPath)(sqlContext)) {
      val df = sqlContext.read.option("header", "true").csv(manifestBlobOrPath)
      ManifestPath(df, manifestBlobOrPath)
    } else {
      logger.warn("Manifest does not exist as a path; parsing as a blob.")
      val csvDataset = sqlContext.createDataset(manifestBlobOrPath.lines.toSeq)
      val df = sqlContext.read.option("header", "true").csv(csvDataset)
      ManifestBlob(df)
    }
    manifest.validateColumns(expectedColumns)
    manifest
  }

  private[projectglow] def getCaseClassFieldsAndTypes[T <: Product: TypeTag]
      : Seq[(String, Type)] = {
    // See https://stackoverflow.com/a/16079804 for an explanation of this logic
    typeOf[T].members.sorted.collect {
      case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType)
    }
  }

  /**
   * Load sample metadata from a manifest.
   *
   * @param manifestBlobOrPath path to the manifest file, or a blob containing the manifest
   * @param sqlContext
   * @return
   */
  def loadSampleMetadata(manifestBlobOrPath: String)(
      implicit sqlContext: SQLContext): Dataset[SampleMetadata] = {
    // read the manifest: expected format = file_path, sample_id, paired_end, read_group_id
    val expectedColumns = getCaseClassFieldsAndTypes[ManifestEntry].map(_._1)
    val manifest = loadCsvManifest(manifestBlobOrPath, expectedColumns)(sqlContext)

    import sqlContext.implicits._
    val manifestEntryDs = manifest
      .contents
      .na
      .fill("")
      .as[ManifestEntry]

    manifestEntryDs.groupByKey(entry => entry.sample_id).mapGroups {
      case (sampleId, iter) =>
        entriesForSample(manifest, sampleId, iter)
    }
  }

  def entriesForSample(
      manifest: Manifest,
      sampleId: String,
      entries: Iterator[ManifestEntry]): SampleMetadata = {
    val readGroups = entries.toIterable.groupBy(_.read_group_id).map {
      case ("", _) => throw new IllegalArgumentException("Read group ID cannot be empty.")
      case (id, inputs) =>
        val (first, second) = inputs.partition(_.paired_end == "1")
        val unpaired = first.isEmpty
        val properlyPaired = (first.size == second.size) && (first.size == 1)
        require(
          unpaired || properlyPaired,
          "For a given sample_id in a manifest file," +
          " there should be one file_path for unpaired reads" +
          " and exactly two file_paths for paired reads (one for each paired end) "
        )

        val (input, secondInput) = if (properlyPaired) {
          val firstInput = manifest.resolve(first.head.file_path)
          val secondInput = manifest.resolve(second.head.file_path)
          (firstInput, Some(secondInput))
        } else {
          (manifest.resolve(second.head.file_path), None)
        }
        ReadGroupMetadata(id, input, secondInput)
    }
    SampleMetadata(sampleId, readGroups.toSeq)
  }

  /**
   * Load file paths from a manifest file.
   *
   * @param manifestBlobOrPath path to the manifest file, or a blob containing the manifest
   * @param sqlContext
   * @return
   */
  def loadFilePaths(manifestBlobOrPath: String)(
      implicit sqlContext: SQLContext): Dataset[String] = {
    require(manifestBlobOrPath.nonEmpty, "Must provide a VCF manifest")

    import sqlContext.implicits._
    val manifest = if (pathExists(manifestBlobOrPath)(sqlContext)) {
      ManifestPath(sqlContext.read.text(manifestBlobOrPath), manifestBlobOrPath)
    } else {
      logger.warn("Manifest does not exist as a path; parsing as a blob.")
      ManifestBlob(sqlContext.createDataset(manifestBlobOrPath.lines.toSeq).toDF)
    }
    manifest.contents.as[String].map { entry =>
      manifest.resolve(entry)
    }
  }
}
