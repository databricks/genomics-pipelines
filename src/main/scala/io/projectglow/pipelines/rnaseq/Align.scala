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

import com.google.common.annotations.VisibleForTesting
import htsjdk.samtools.ValidationStringency
import io.projectglow.common.logging._
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.models.{ReadGroup, SequenceDictionary}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.{FragmentDataset, InterleavedFASTQInFormatter}
import org.bdgenomics.adam.rdd.read.{AlignmentDataset, AnySAMOutFormatter}
import org.bdgenomics.adam.sql.{Alignment => AlignmentProduct}
import org.bdgenomics.formats.avro.{Alignment, ProcessingStep}
import io.projectglow.pipelines.common.{BamSaver, DirectoryHelper, VersionTracker}
import io.projectglow.pipelines.common.{BamSaver, VersionTracker}
import io.projectglow.pipelines.dnaseq.{HasAlignmentOutput, HasBamOutput, HasDBNucleusHome, HasOutput, HasReferenceGenome, ReadGroupMetadata, SampleMetadata, SingleSampleStage}

class Align
    extends SingleSampleStage
    with HasAlignmentOutput
    with HasBamOutput
    with HasReferenceGenome
    with HasOutput
    with HlsUsageLogging
    with HasDBNucleusHome {

  final val starHome = new Param[String](this, "starHome", "STAR Home Directory")

  def setStarHome(value: String): this.type = set(starHome, value)
  def getStarHome: String = get(starHome).getOrElse(s"$getHome/star")

  override def description(): String = {
    "Alignment using STAR/ADAM"
  }

  override def init(): Unit = {
    super.init()
    val sampleId = getSampleMetadata.sample_id
    val alignmentOutputRoot = s"$getOutput/aligned"
    set(alignmentOutput, s"$alignmentOutputRoot/recordGroupSample=$sampleId")
    set(alignmentOutputBam, s"$getOutput/aligned.bam/$sampleId.bam")
  }

  override def outputExists(session: SparkSession): Boolean = {
    DirectoryHelper.directoryWasCommitted(session, $(alignmentOutput))
  }

  override def getOutputParams: Seq[Param[_]] = Seq(alignmentOutput)

  private def loadFragments(input: String, secondInput: Option[String])(
      implicit sc: SparkContext) = {

    val minInputPartitions = sc.defaultParallelism
    val reads = sc
      .loadAlignments(input, secondInput, stringency = ValidationStringency.SILENT)
      .transformDataset { ds =>
        // if there isn't enough parallelism, repartition the input
        // this commonly occurs with non splittable FASTQ files
        val numPartitions = ds.rdd.partitions.length
        if (numPartitions < minInputPartitions) {
          ds.repartition(minInputPartitions)
        } else {
          ds
        }
      }

    val fragments = reads.transformDataset { ds =>
      import ds.sparkSession.implicits._
      ds.withColumn(
          "readName",
          split(col("readName"), " ")
            .getItem(0)
        )
        .as[AlignmentProduct]
    }.toFragments

    fragments
  }

  override def execute(session: SparkSession): Unit = {
    implicit val sc: SparkContext = session.sparkContext

    val SampleMetadata(sampleId, readGroups) = getSampleMetadata
    val fragmentsByReadGroup = readGroups.map {
      case ReadGroupMetadata(readGroupId, input, secondInput) =>
        (readGroupId, loadFragments(input, secondInput))
    }

    // Get the sequences from the reference build
    val refPath = getPrebuiltReferenceGenomePath.stripSuffix("/")
    val sdPath = s"file://$refPath/$getReferenceGenomeName.dict"

    // Get the sequences from the reference build
    val sequences = sc.loadSequenceDictionary(sdPath)

    // clear out the sequence dictionary to enforce a naive partitioning
    val alignedReads = runAlignment(sampleId, fragmentsByReadGroup, sequences)

    val previousId = alignedReads.processingSteps.lastOption.map(_.getId)
    val withProcessingSteps = alignedReads
      .replaceProcessingSteps(
        alignedReads.processingSteps ++ processingSteps(session, previousId)
      )

    BamSaver.saveToParquet(withProcessingSteps, $(alignmentOutput))
    if ($(exportBam)) {
      BamSaver.writeBam(
        sc,
        $(alignmentOutput),
        $(alignmentOutputBam),
        $(exportBamAsSingleFile),
        $(sortOnSave)
      )
    }
  }

  private def runAlignment(
      sample: String,
      fragmentsByReadGroup: Seq[(String, FragmentDataset)],
      sequences: SequenceDictionary): AlignmentDataset = {

    val starDir = getStarHome.stripSuffix("/")

    implicit val tFormatter = InterleavedFASTQInFormatter
    implicit val uFormatter = AnySAMOutFormatter(ValidationStringency.LENIENT)

    // clear out the sequence dictionary to enforce a naive partitioning
    // align each read group separately and union the results
    // it would be exceptionally unusual for someone to run multiple read groups
    // of RNA-seq data, but, exceptionally unusual == inevitably someone will
    fragmentsByReadGroup.map {
      case (readGroupId, fragments) =>
        val recordGroupName = if (readGroupId.trim.isEmpty) sample else readGroupId

        fragments
          .replaceSequences(SequenceDictionary.empty)
          .pipe[
            Alignment,
            AlignmentProduct,
            AlignmentDataset,
            InterleavedFASTQInFormatter
          ](Seq(s"$starDir/star-runner.sh", getPrebuiltReferenceGenomePath))
          .addSequences(sequences)
          .addReadGroup(ReadGroup(sample, recordGroupName, library = Some(sample)))
          .transformDataFrame { df =>
            df.drop("readGroupId")
              .withColumn("readGroupId", lit(recordGroupName))
          }
    }.reduce(_.union(_))
  }

  @VisibleForTesting
  private[rnaseq] def processingSteps(
      sess: SparkSession,
      previousId: Option[String]): Seq[ProcessingStep] = {
    val versionTracker = new VersionTracker(sess, "RNASeq")
    versionTracker.addStep(
      "STAR",
      "STAR",
      s"refGenomeName=$getPrebuiltReferenceGenomeName"
    )

    if ($(sortOnSave)) {
      versionTracker.addStep("Sort", "ADAM")
    }
    versionTracker.build(previousId)
  }
}
