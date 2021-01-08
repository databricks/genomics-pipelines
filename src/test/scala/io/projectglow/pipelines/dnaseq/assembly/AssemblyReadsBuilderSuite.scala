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

import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import io.projectglow.pipelines.{Pipeline, PipelineBaseTest}

class AssemblyReadsBuilderSuite extends PipelineBaseTest {

  val minimumMappingQuality = 20

  def checkDifference(ds1: Dataset[_], ds2: Dataset[_], difference: Int) {
    assert(ds1.count - ds2.count === difference)
  }

  def checkDifference(rdd: AlignmentDataset, ds2: Dataset[_], difference: Int) {
    checkDifference(rdd.dataset, ds2, difference)
  }

  test("filter reads from 21:10002403") {
    val alignmentOutput = s"$testDataHome/NA12878_21_10002403.bam"

    val sc = spark.sparkContext
    val reads = sc.loadAlignments(alignmentOutput)

    checkDifference(
      reads,
      new AssemblyReadsBuilder(reads.dataset)
        .filterReadsByQuality(minimumMappingQuality)
        .build(),
      668
    )
  }

  test("filter reads from 20:10000117") {
    val alignmentOutput = s"$testDataHome/NA12878_20_10000117.bam"

    val sc = spark.sparkContext
    val reads = sc.loadAlignments(alignmentOutput)

    checkDifference(
      reads,
      new AssemblyReadsBuilder(reads.dataset)
        .filterReadsByQuality(minimumMappingQuality)
        .build(),
      46
    )
  }

  test("filter reads from 21:10085128") {
    val alignmentOutput = s"$testDataHome/NA12878_21_10085128.bam"

    val sc = spark.sparkContext
    val reads = sc.loadAlignments(alignmentOutput)

    checkDifference(
      reads,
      new AssemblyReadsBuilder(reads.dataset)
        .filterReadsByQuality(minimumMappingQuality)
        .build(),
      805
    )
  }

  test("filter reads from 21:10024349") {
    val alignmentOutput = s"$testDataHome/NA12878_21_10024349.bam"

    val sc = spark.sparkContext
    val reads = sc.loadAlignments(alignmentOutput)

    checkDifference(
      reads,
      new AssemblyReadsBuilder(reads.dataset)
        .filterReadsByQuality(minimumMappingQuality)
        .build(),
      332
    )
  }

  test("filter reads from 21:10003099") {
    val alignmentOutput = s"$testDataHome/NA12878_21_10003099.bam"

    val sc = spark.sparkContext
    val reads = sc.loadAlignments(alignmentOutput)

    checkDifference(
      reads,
      new AssemblyReadsBuilder(reads.dataset)
        .filterReadsByQuality(minimumMappingQuality)
        .build(),
      434
    )
  }

  test("filtering without regions is a no op") {
    val readPath = s"$testDataHome/NA12878_21_10002403.bam"
    val sc = spark.sparkContext
    val reads = sc.loadAlignments(readPath).dataset

    val filteredReads = new AssemblyReadsBuilder(reads).build()

    assert(filteredReads.count === reads.count)
  }

  test("filter regions from a file") {
    Pipeline.setPipelineConfigs(spark)
    val readPath = s"$testDataHome/NA12878_21_10002403.bam"
    val sc = spark.sparkContext
    val reads = sc.loadAlignments(readPath).dataset

    // bed contains 21:10002399-10002410 (note: bed is 1-based inclusive)
    val bedPath = s"$testDataHome/21_10002403.bed"
    val targets = sc.loadFeatures(bedPath)

    val filteredReads = new AssemblyReadsBuilder(reads)
      .filterTargetedReads(targets)
      .build()

    assert(filteredReads.count === 465)
  }
}
