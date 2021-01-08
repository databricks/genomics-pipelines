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

import java.io.File
import java.nio.file.Files

import io.projectglow.pipelines.PipelineBaseTest
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentDataset

class BamSaverSuite extends PipelineBaseTest {

  lazy val alignments: AlignmentDataset = {
    spark
      .sparkContext
      .loadAlignments(s"$testDataHome/NA12878_21_10003099.bam")
      .transformDataset { ds =>
        ds.repartition(3)
          .sortWithinPartitions(
            col("referenceName").desc_nulls_first,
            col("start").desc_nulls_first
          )
      }
  }

  def writeWithAdam(alignments: AlignmentDataset, sort: Boolean): File = {
    val tempFile = Files.createTempFile("adam", ".bam")
    val sortedAlignments = alignments.transformDataset { ds =>
      ds.sort(
        col("referenceName").asc_nulls_last,
        col("start").asc_nulls_last,
        col("readName"),
        col("readInFragment")
      )
    }
    sortedAlignments.saveAsSam(
      tempFile.toString,
      asSingleFile = true,
      isSorted = sort,
      deferMerging = false
    )
    tempFile.toFile
  }

  def writeWithDatabricks(alignments: AlignmentDataset, sort: Boolean): File = {
    val tempFile = Files.createTempFile("databricks", ".bam")
    BamSaver.writeBigBam(alignments, tempFile.toString, sort)
    tempFile.toFile
  }

  gridTest("Compare single-file BAM writers")(Seq(true, false)) { sort =>
    assert(alignments.rdd.getNumPartitions > 1) // Should have 3 partitions
    val adamBam = writeWithAdam(alignments, sort)
    val databricksBam = writeWithDatabricks(alignments, sort)
    FileUtils.contentEquals(adamBam, databricksBam)
  }
}
