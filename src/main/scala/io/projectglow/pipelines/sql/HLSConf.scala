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

package io.projectglow.pipelines.sql

import org.apache.spark.sql.internal.SQLConf.buildConf

/**
 * Configuration for HLS Specific SQL Features
 */
object HLSConf {
  // Default number of partitions used for several pipeline stages
  private val defaultNumPartitions = 1024

  val ASSEMBLY_REGION_BIN_SIZE =
    buildConf("spark.databricks.hls.pipeline.dnaseq.assemblyRegionBinSize")
      .internal()
      .doc("The Bin Size to choose for Partitioning Assembly Reads")
      .intConf
      .createWithDefault(5000)

  val ASSEMBLY_REGION_NUM_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.dnaseq.assemblyRegionNumPartitions")
      .internal()
      .doc("The number of partitions for Assembly Reads")
      .intConf
      .createWithDefault(defaultNumPartitions)

  val JOINT_GENOTYPING_BIN_SIZE =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingBinSize")
      .internal()
      .doc("The Bin Size to choose for Partitioning Variant Contexts in Joint Genotyping")
      .intConf
      .createWithDefault(5000)

  val JOINT_GENOTYPING_MAX_PUSHDOWN_FILTERS =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingMaxPushdownFilters")
      .internal()
      .doc(
        "The maximum number of filters to push down in Joint Genotyping. With a default-sized " +
        "stack, the maximum number before encountering a stack overflow is 122."
      )
      .intConf
      .createWithDefault(25)

  val JOINT_GENOTYPING_NUM_BIN_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingNumBinPartitions")
      .internal()
      .doc("The number of partitions for binning Variant Contexts in Joint Genotyping")
      .intConf
      .createWithDefault(6000)

  val JOINT_GENOTYPING_NUM_SHUFFLE_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingNumPartitions")
      .internal()
      .doc("The number of partitions for shuffles in Joint Genotyping")
      .intConf
      .createWithDefault(3000)

  val JOINT_GENOTYPING_RANGE_JOIN_BIN_SIZE =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingRangeJoinBinSize")
      .internal()
      .doc(
        "The Bin Size to choose for Range Join on Variant Loci and Variant Contexts " +
        "in Joint Genotyping"
      )
      .intConf
      .createWithDefault(1000)

  // Default matches GenotypeLikelihoods.MAX_DIPLOID_ALT_ALLELES_THAT_CAN_BE_GENOTYPED - 1
  val JOINT_GENOTYPING_MAX_NUM_ALT_ALLELES =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingMaxNumAltAlleles")
      .internal()
      .doc(
        "The number of alternate alleles above which loci are skipped in Joint Genotyping"
      )
      .intConf
      .createWithDefault(49)

  val PIPELINE_NUM_CONCURRENT_SAMPLES =
    buildConf("spark.databricks.hls.pipeline.dnaseq.numConcurrentSamples")
      .internal()
      .doc("The Number of concurrent samples to process")
      .intConf
      .createWithDefault(1) // for exomes raise this number to process more samples in parallel

  val VARIANT_INGEST_BATCH_SIZE =
    buildConf("spark.databricks.hls.pipeline.joint.variantIngestBatchSize")
      .internal()
      .doc("Number of gVCFs to read as a batch during variant ingest")
      .intConf
      .createWithDefault(500)

  val VARIANT_INGEST_NUM_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.joint.variantIngestNumPartitions")
      .internal()
      .doc("Number of partitions for coalescing gVCF datasets during variant ingest")
      .intConf
      .createWithDefault(3000)

  val VARIANT_INGEST_SKIP_STATS_COLLECTION =
    buildConf("spark.databricks.hls.pipeline.joint.variantIngestSkipStatsCollection")
      .internal()
      .doc("Whether to skip stats collection for Delta data skipping")
      .booleanConf
      .createWithDefault(true)

  val ALIGNMENT_MIN_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.dnaseq.alignmentMinPartitions")
      .internal()
      .doc(
        "Minimum number of partitions to use for DNA alignment. The number of partitions used is" +
        "max(this value, 3 * the number of file scan partitions). The number of file scan " +
        "partitions depends on the cluster size and total input size. Set to 1024 by default " +
        "to fully utilize a 500 core cluster, which is a common approximate size, as well " +
        "match the default number of variant calling partitions."
      )
      .intConf
      .createWithDefault(defaultNumPartitions)

  val ALIGNMENT_NUM_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.dnaseq.alignmentNumPartitions")
      .internal()
      .doc(
        "Exact number of partitions to use for alignment. Primarily used for testing since " +
        "alignment results can differ based on reads in a BWA-MEM batch. If provided, this value" +
        "overrides alignmentMinPartitions."
      )
      .intConf
      .createOptional

  val BWA_MEM_JNI_BATCH_SIZE_BASES =
    buildConf("spark.databricks.hls.pipeline.dnaseq.bwaMemJniBatchSizeBases")
      .internal()
      .doc("How many bases to pass to BWA in each JNI call.")
      .intConf
      .createWithDefault(1000000)

  val MARK_DUPLICATES_NUM_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.dnaseq.markDuplicatesNumPartitions")
      .internal()
      .doc("Number of partitions to use for duplicate marking")
      .intConf
      .createWithDefault(defaultNumPartitions)
}
