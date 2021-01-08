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

package io.projectglow.pipelines.rnaseq

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.bdgenomics.adam.models.{ReadGroup, ReadGroupDictionary, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureDataset
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.formats.avro.Alignment
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.common.DirectoryHelper
import io.projectglow.pipelines.dnaseq.SampleMetadata

case class ReadCount(
    transcriptId: String,
    reads: Long,
    referenceName: String,
    start: Long,
    end: Long,
    sampleId: String)

object ReadCount {

  def fromFeatures(features: FeatureDataset): Dataset[ReadCount] = {
    import features.dataset.sqlContext.implicits._
    features
      .dataset
      .select(
        $"transcriptId",
        $"score".cast(LongType).as("reads"),
        $"referenceName",
        $"start",
        $"end",
        $"source".as("sampleId")
      )
      .as[ReadCount]
  }
}

class CountSuite extends PipelineBaseTest {

  val grch38chr1 = SequenceDictionary(SequenceRecord("chr1", 248956422L))
  val recordGroups = ReadGroupDictionary(Seq(ReadGroup("mySample", "mySample")))

  def makeReads(sc: SparkContext, reads: Alignment*): AlignmentDataset = {

    AlignmentDataset(sc.parallelize(reads.toSeq), grch38chr1, recordGroups, Seq())
  }

  def filterAndCount(reads: AlignmentDataset, sc: SparkContext): DataFrame = {
    val transcriptomePath = s"$testDataHome/rnaseq/ENSG00000223972.5.gtf"
    val counter = new Count()

    val transcriptome = sc.loadFeatures(transcriptomePath)
    counter.countReadsPerTranscript(reads, counter.filterExons(transcriptome))
  }

  def filterAndCountAsFeatures(reads: AlignmentDataset, sc: SparkContext): FeatureDataset = {
    val transcriptomePath = s"$testDataHome/rnaseq/ENSG00000223972.5.gtf"
    val counter = new Count()

    val transcriptome = sc.loadFeatures(transcriptomePath)
    counter.convertCountsToFeatures(
      counter.filterTranscripts(transcriptome),
      counter.countReadsPerTranscript(reads, counter.filterExons(transcriptome)),
      reads.readGroups.toSamples.head.getId
    )
  }

  def makeRead(start: Long, end: Long): Alignment = {
    Alignment
      .newBuilder
      .setReferenceName("chr1")
      .setStart(start)
      .setEnd(end)
      .setReadGroupSampleId("mySample")
      .build
  }

  test("filter down to exons") {
    val transcriptomePath = s"$testDataHome/rnaseq/ENSG00000223972.5.gtf"

    val sc = spark.sparkContext
    val counter = new Count()

    val transcriptome = sc.loadFeatures(transcriptomePath)
    assert(transcriptome.dataset.count === 12)
    assert(counter.filterExons(transcriptome).dataset.count === 9)
  }

  test("filter down to transcripts") {
    val transcriptomePath = s"$testDataHome/rnaseq/ENSG00000223972.5.gtf"

    val sc = spark.sparkContext
    val counter = new Count()

    val transcriptome = sc.loadFeatures(transcriptomePath)
    assert(transcriptome.dataset.count === 12)
    assert(counter.filterTranscripts(transcriptome).dataset.count === 2)
  }

  test("count a read mapping to a single exon in a single transcript") {
    val sc = spark.sparkContext

    val reads = makeReads(sc, makeRead(11880L, 12000L))
    val counts = filterAndCount(reads, sc).collect

    assert(counts.size === 1)
    assert(counts.head === Row("ENST00000456328.2", 1))
  }

  test("count a read mapping to multiple transcripts") {
    val sc = spark.sparkContext

    val reads = makeReads(sc, makeRead(12620L, 12680L))
    val counts = filterAndCount(reads, sc).collect

    assert(counts.size === 2)
    val countSet = counts.toSet
    assert(countSet.size === 2)
    assert(countSet(Row("ENST00000456328.2", 1)))
    assert(countSet(Row("ENST00000450305.2", 1)))
  }

  test("count a read which maps to no transcripts") {
    val sc = spark.sparkContext

    val reads = makeReads(sc, makeRead(12300L, 12400L))
    val counts = filterAndCount(reads, sc).collect

    assert(counts.size === 0)
  }

  test("count reads mapping to various exons, save, and load") {
    val sc = spark.sparkContext
    val outputDir = Files.createTempDirectory("output")

    val readPath = s"$testDataHome/rnaseq/SRR6591970.chr1_11869_14409.sam"

    val reads = sc.loadAlignments(readPath)
    filterAndCountAsFeatures(reads, sc)
      .saveAsParquet(outputDir + "/transcript_counts")

    val loadedCounts = sc.loadFeatures(outputDir + "/transcript_counts")

    assert(loadedCounts.dataset.count === 2)

    val countSet = ReadCount.fromFeatures(loadedCounts).collect.toSet
    assert(countSet(ReadCount("ENST00000456328.2", 52, "chr1", 11868L, 14409L, "SRR6591970")))

    assert(countSet(ReadCount("ENST00000450305.2", 23, "chr1", 12009L, 13670L, "SRR6591970")))
  }

  test("end to end test") {
    val outputDir = Files.createTempDirectory("output").toString
    val readPath = s"$testDataHome/rnaseq/SRR6591970.chr1_11869_14409.sam"
    val transcriptomePath = s"$testDataHome/rnaseq/ENSG00000223972.5.gtf"
    val sampleMetadata = SampleMetadata("SRR6591970", Seq.empty)

    val stage = new Count()
      .setAlignmentOutput(readPath)
      .setOutput(outputDir)
      .setTranscriptomePath(transcriptomePath)
      .setSampleMetadata(sampleMetadata)
    stage.init()
    stage.execute(spark) // Run end-to-end to verify wiring is correct
  }

  test("replay mode") {
    val stage = new Count()
      .setOutput(Files.createTempDirectory("replay").toString)
      .setSampleMetadata(SampleMetadata("sample", Seq.empty))
    stage.init()
    assert(!stage.outputExists(spark))
    val dir = Paths.get(stage.getCountsPerTranscriptBySampleOutput)
    Files.createDirectories(dir)
    Files.createFile(dir.resolve(DirectoryHelper.DIRECTORY_COMMIT_SUCCESS_FILE))
    assert(stage.outputExists(spark))
  }
}
