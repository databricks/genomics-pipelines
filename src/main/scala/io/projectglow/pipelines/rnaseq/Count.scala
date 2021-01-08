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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hls.dsl.expressions.overlaps
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureDataset
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import io.projectglow.common.logging._
import io.projectglow.pipelines.common.DirectoryHelper
import io.projectglow.pipelines.dnaseq.{HasAlignmentOutput, HasOutput, HasReferenceGenome}
import io.projectglow.pipelines.dnaseq.{HasAlignmentOutput, HasOutput, HasReferenceGenome, SingleSampleStage}

class Count
    extends SingleSampleStage
    with HasAlignmentOutput
    with HasReferenceGenome
    with HlsUsageLogging
    with HasOutput {

  val countsPerTranscriptBySampleOutput = new Param[String](
    this,
    "countsPerTranscriptBySampleOutput",
    "Output of read counts per transcript"
  )

  val transcriptomePath =
    new Param[String](parent = this, name = "transcriptomePath", doc = "Path to transcriptome gtf")

  def setTranscriptomePath(path: String): this.type = set(transcriptomePath, path)
  def getCountsPerTranscriptBySampleOutput: String = $(countsPerTranscriptBySampleOutput)

  private def gtfForReferenceGenome(refGenomeId: String): String = {
    get(transcriptomePath).getOrElse {
      refGenomeId match {
        case GRCH37Star => s"file://$getPrebuiltReferenceGenomePath/Homo_sapiens.GRCh37.75.gtf"
        case GRCH38Star => s"file://$getPrebuiltReferenceGenomePath/Homo_sapiens.GRCh38.93.chr.gtf"
        case _ => s"file://$getPrebuiltReferenceGenomePath.gtf" // fallback for tests
      }
    }
  }

  override def init(): Unit = {
    super.init()
    val sampleId = getSampleMetadata.sample_id
    val quantificationOutputRoot = s"$getOutput/quantification"
    set(countsPerTranscriptBySampleOutput, s"$quantificationOutputRoot/sampleId=$sampleId")
  }

  override def outputExists(session: SparkSession): Boolean = {
    DirectoryHelper.directoryWasCommitted(session, $(countsPerTranscriptBySampleOutput))
  }

  def filterTranscripts(transcriptome: FeatureDataset): FeatureDataset = {
    transcriptome.transformDataFrame(df => {
      import df.sqlContext.implicits._

      df.where($"featureType" === "transcript")
    })
  }

  def filterExons(transcriptome: FeatureDataset): FeatureDataset = {
    transcriptome.transformDataFrame(df => {
      import df.sqlContext.implicits._

      df.where($"featureType" === "exon")
    })
  }

  def countReadsPerTranscript(reads: AlignmentDataset, transcripts: FeatureDataset): DataFrame = {

    val transcriptsDs = transcripts.dataset
    val readsDs = reads.dataset
    import readsDs.sqlContext.implicits._

    val transcriptsDf = transcriptsDs
      .withColumnRenamed("referenceName", "featureContigName")
      .withColumnRenamed("start", "featureStart")
      .withColumnRenamed("end", "featureEnd")

    val readsDf = readsDs.select(
      $"referenceName".as("readContigName"),
      $"start".as("readStart"),
      $"end".as("readEnd")
    )

    val joinCond = readsDf("readContigName") === transcriptsDf("featureContigName") &&
      overlaps(
        readsDf("readStart"),
        readsDf("readEnd"),
        transcriptsDf("featureStart"),
        transcriptsDf("featureEnd")
      )
    val readsMappedToTranscripts = transcriptsDf
      .join(readsDf, joinCond)
      .select("transcriptId")

    readsMappedToTranscripts
      .groupBy("transcriptId")
      .count()
  }

  def convertCountsToFeatures(
      transcripts: FeatureDataset,
      counts: DataFrame,
      sampleId: String): FeatureDataset = {
    transcripts.transformDataFrame(tDf => {

      import tDf.sqlContext.implicits._

      tDf
        .join(counts, Seq("transcriptId"), "left_outer")
        .withColumn("score", when($"count".isNull, lit(0.0)).otherwise($"count".cast(DoubleType)))
        .withColumn("source", lit(sampleId))
    })
  }

  def saveReadCounts(counts: FeatureDataset, sc: SparkContext) {

    val path = new Path($(countsPerTranscriptBySampleOutput))
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    fs.delete(path, true)

    counts
      .transformDataset(_.orderBy("referenceName", "start", "end"))
      .saveAsParquet($(countsPerTranscriptBySampleOutput))
  }

  override def getOutputParams: Seq[Param[_]] = Seq(countsPerTranscriptBySampleOutput)

  override def execute(session: SparkSession): Unit = {

    val sc = session.sparkContext

    // load in reads
    val reads = sc.loadAlignments($(alignmentOutput))

    val samples = reads.readGroups.toSamples
    require(samples.size == 1, "Should only have one sample. Saw: " + samples.mkString(","))
    val sampleId = samples.head.getId

    // load in transcriptome build
    val transcriptome = sc.loadFeatures(
      gtfForReferenceGenome($(refGenomeId)),
      optSequenceDictionary = Option(reads.sequences)
    )

    // exclude records in the transcriptome build that are not exons
    val codingSequences = filterExons(transcriptome)

    // count reads per transcript
    val readCounts = countReadsPerTranscript(reads, codingSequences)

    // join counts back against transcript info
    val countsAsFeatures =
      convertCountsToFeatures(filterTranscripts(transcriptome), readCounts, sampleId)

    // save read counts out
    saveReadCounts(countsAsFeatures, sc)
  }
}
