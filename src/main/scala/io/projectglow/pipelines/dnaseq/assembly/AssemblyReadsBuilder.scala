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

package io.projectglow.pipelines.dnaseq.assembly

import io.projectglow.pipelines.sql.HLSConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.hls.dsl.expressions.overlaps
import org.apache.spark.sql.internal.SQLConf
import org.bdgenomics.adam.rdd.feature.FeatureDataset
import org.bdgenomics.adam.sql.Alignment

/**
 * Applies a number of transformations to the Aligned Reads
 * via a builder pattern to put them in the form expected by GATK's
 * Haplotype Caller.
 *
 * We apply the following read filters:
 *
 * * Duplicate read filter: We discard any reads that are marked as likely
 *   PCR/optical duplicates.
 * * Mapping quality filter: We discard any reads that have a low mapping
 *   quality (default is MAPQ < 20).
 * * Sequence length filter: We discard any reads that contain fewer than ten
 *   bases of sequence. These reads cannot be confidently mapped.
 *
 * We also filter out reads that do not belong in the targeted regions.
 *
 * @param reads
 */
class AssemblyReadsBuilder(reads: Dataset[Alignment]) {

  /**
   * Filter reads within targeted regions
   *
   * @param targetRegions
   * @return
   */
  def filterTargetedReads(targetRegions: FeatureDataset): AssemblyReadsBuilder = {
    import reads.sqlContext.implicits._
    val targets = targetRegions.dataset
    val binSize = SQLConf.get.getConf(HLSConf.ASSEMBLY_REGION_BIN_SIZE)
    val joinCond = reads("referenceName") === targets("referenceName") &&
      overlaps(reads("start"), reads("end"), targets("start"), targets("end"))

    // Note: we write this as an inner join and dropDuplicates instead of a left_semi join
    // because the DBR range join optimization does not apply to left_semi joins. See SC-16511.
    val transformedReads = reads
      .hint("range_join", binSize)
      .join(targets, joinCond, "inner")
      .select(reads("*"))
      .dropDuplicates()
      .as[Alignment]
    new AssemblyReadsBuilder(transformedReads)
  }

  /**
   * Filters alignments based on a mapping quality.
   *
   * Also includes basic read filters that are common to HaplotypeCaller and Mutect2. These filters
   * are written as simple Spark SQL expressions here, but are also implemented downstream in
   * [[HaplotypeAssemblyShardCaller]] and [[Mutect2ShardCaller]].
   *
   * @param mappingQualityThreshold Minimum read mapping quality
   * @return
   */
  def filterReadsByQuality(mappingQualityThreshold: Int): AssemblyReadsBuilder = {
    val transformedReads = reads
      .where(col("mappingQuality") >= mappingQualityThreshold)
      .where(col("readMapped"))
      .where(!col("secondaryAlignment"))
      .where(!col("duplicateRead"))
      .where(!col("failedVendorQualityChecks"))
    new AssemblyReadsBuilder(transformedReads)
  }

  def build(): Dataset[Alignment] = reads
}
