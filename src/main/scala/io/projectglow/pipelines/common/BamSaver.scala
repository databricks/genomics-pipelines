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

import java.io.ByteArrayOutputStream

import io.projectglow.sql.SingleFileWriter
import htsjdk.samtools.{BAMFileWriter, BAMRecordCodec}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.seqdoop.hadoop_bam.util.DatabricksBGZFOutputStream

object BamSaver {

  // Deletes existing Parquet output if present
  def saveToParquet(alignments: AlignmentDataset, parquetOutput: String): Unit = {

    val sc = alignments.dataset.sparkSession.sparkContext

    val path = new Path(parquetOutput)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    fs.delete(path, true)
    alignments.saveAsParquet(parquetOutput)
  }

  // Reads alignments from ADAM Parquet output, sorts them if requested, and writes to BAM
  def writeBam(
      sparkContext: SparkContext,
      parquetOutput: String,
      bamOutput: String,
      exportBamAsSingleFile: Boolean,
      sortBamOutput: Boolean): Unit = {

    val alignments = sparkContext.loadAlignments(parquetOutput)
    val outputPath = new Path(bamOutput)
    val fs = outputPath.getFileSystem(alignments.rdd.sparkContext.hadoopConfiguration)
    fs.delete(outputPath, true)

    val maybeSortedAlignments = if (sortBamOutput) {
      alignments.transformDataset { ds =>
        ds.sort(
          col("referenceName").asc_nulls_last,
          col("start").asc_nulls_last,
          col("readName"),
          col("readInFragment")
        )
      }
    } else {
      alignments
    }

    if (exportBamAsSingleFile) {
      writeBigBam(maybeSortedAlignments, bamOutput, sortBamOutput)
    } else {
      writeShardedBam(maybeSortedAlignments, bamOutput, sortBamOutput)
    }
  }

  /**
   * Writes a ADAM [[AlignmentDataset]] to sharded BAMs.
   *
   * @param alignmentDataset Alignments to save
   * @param path BAM files to save
   * @param isSorted Whether the alignments are sorted
   */
  def writeShardedBam(alignmentDataset: AlignmentDataset, path: String, isSorted: Boolean): Unit = {

    alignmentDataset.saveAsSam(
      path,
      asSingleFile = false,
      isSorted = isSorted,
      deferMerging = false
    )
  }

  /**
   * Writes a ADAM [[AlignmentDataset]] to a single-file BAM.
   * By not relying on the file-merging functionality of single-file writing in ADAM, this function
   * avoids non-deterministic issues in cloud storage.
   *
   * @param alignmentDataset Alignments to save
   * @param path BAM file to save
   * @param isSorted Whether the alignments are sorted
   */
  def writeBigBam(alignmentDataset: AlignmentDataset, path: String, isSorted: Boolean): Unit = {
    val (header, records) = alignmentDataset.convertToSam(isSorted)
    val nParts = records.getNumPartitions

    val byteArrayRdd = records.mapPartitionsWithIndex {
      case (idx, iter) =>
        val baos = new ByteArrayOutputStream()
        val bgzfOutputStream = new DatabricksBGZFOutputStream(baos)
        val bamCodec = new BAMRecordCodec(header)
        bamCodec.setOutputStream(bgzfOutputStream)

        // Write an empty GZIP block iff this is the last partition
        DatabricksBGZFOutputStream.setWriteEmptyBlockOnClose(bgzfOutputStream, idx == nParts - 1)

        if (idx == 0) {
          BAMFileWriter.writeHeader(baos, header)
        }

        iter.foreach { samRecord =>
          bamCodec.encode(samRecord.get())
        }

        bgzfOutputStream.close()
        Iterator(baos.toByteArray)
    }

    val conf = alignmentDataset.dataset.sparkSession.sessionState.newHadoopConf()
    SingleFileWriter.write(byteArrayRdd, path, conf)
  }
}
