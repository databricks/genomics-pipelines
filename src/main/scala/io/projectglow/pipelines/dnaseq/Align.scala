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

import scala.collection.mutable
import scala.util.control.NonFatal
import com.google.common.annotations.VisibleForTesting
import htsjdk.samtools.ValidationStringency
import io.projectglow.common.logging._
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.{FastSnpTable, ReadGroup, ReadGroupDictionary, SequenceDictionary, SnpTable}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.{DatasetBoundFragmentDataset, FragmentDataset}
import org.bdgenomics.adam.rdd.read.{AlignmentDataset, QualityScoreBin}
import org.bdgenomics.adam.sql.{Alignment => AlignmentProduct}
import org.bdgenomics.formats.avro.ProcessingStep
import org.broadinstitute.hellbender.utils.bwa._
import io.projectglow.pipelines.UserVisible
import io.projectglow.pipelines.dnaseq.alignment._
import io.projectglow.pipelines.dnaseq.util.AlignmentUtil
import io.projectglow.pipelines.common.{BamSaver, DirectoryHelper, VersionTracker}
import io.projectglow.pipelines.common.{BamSaver, VersionTracker}
import io.projectglow.pipelines.dnaseq.alignment.{AlignmentInput, AlignmentIterator, BwaAligner, ConversionUtils, MarkDuplicates}
import io.projectglow.pipelines.dnaseq.util.AlignmentUtil
import io.projectglow.pipelines.sql.HLSConf

class Align(override val outputSuffix: String = "")
    extends SingleSampleStage
    with HasBamOutput
    with HasAlignmentOutput
    with HasReferenceGenome
    with HasOutput
    with HlsUsageLogging
    with HasDBNucleusHome {

  @UserVisible final val pairedEndMode =
    new BooleanParam(this, "pairedEndMode", "If true, runs BWA in paired-end mode.")

  @UserVisible final val markDuplicates =
    new BooleanParam(this, "markDuplicates", "If true, marks duplicate reads after alignment.")

  @UserVisible final val functionalEquivalence = new BooleanParam(
    this,
    "functionalEquivalence",
    "If true, runs the functional equivalence spec pipeline including " +
    "Duplicate Marking, BQSR and Quality Score Binning."
  )

  @UserVisible final val recalibrateBaseQualities = new BooleanParam(
    this,
    "recalibrateBaseQualities",
    "If true, recalibrates base qualities after duplicate marking/alignment."
  )

  @UserVisible
  final val knownSites =
    new Param[String](
      this,
      "knownSites",
      "Path in cloud storage or DBFS to sites to treat as known SNPs. May be a VCF or an " +
      "ADAM variant dataset. You must set this parameter to recalibrate base qualities with " +
      "a custom reference genome."
    )

  @UserVisible final val binQualityScores = new BooleanParam(
    this,
    "binQualityScores",
    "If provided, bins quality scores to save space on storage."
  )

  final val qualityScoreBinSpec =
    new Param[String](this, "qualityScoreBinSpec", "Bins to apply to quality scores.")

  setDefault(pairedEndMode, true)
  setDefault(markDuplicates, false)
  setDefault(functionalEquivalence, false)
  setDefault(recalibrateBaseQualities, false)
  setDefault(binQualityScores, false)
  setDefault(
    qualityScoreBinSpec,
    Seq("0,2,1", "2,3,2", "3,4,3", "4,5,4", "5,6,5", "6,7,6", "7,16,10", "16,26,20", "26,254,30")
      .mkString(";")
  )
  setDefault(knownSites, "")

  def setPairedEndMode(value: Boolean): this.type = set(pairedEndMode, value)

  def setMarkDuplicates(value: Boolean): this.type = set(markDuplicates, value)

  def setFunctionalEquivalence(value: Boolean): this.type = {
    set(functionalEquivalence, value)
  }

  def setKnownSites(value: String): this.type = set(knownSites, value)

  def getKnownSites: String =
    // Note: default parameter value is empty string, which means that we should use the built in
    // known-sites file. The parameter needs a default so that the pipeline notebook doesn't
    // require it.
    get(knownSites)
      .filter(_.nonEmpty)
      .getOrElse(s"file:$getPrebuiltReferenceGenomePath/known-sites.vcf.bgz")

  def setBinQualityScores(value: Boolean): this.type = {
    set(binQualityScores, value)
  }

  def setQualityScoreBinSpec(value: String): this.type = {
    set(qualityScoreBinSpec, value)
  }

  override def description(): String = {
    "Alignment using BWA/ADAM"
  }

  override def init(): Unit = {
    super.init()
    val sampleId = getSampleMetadata.sample_id
    val alignmentOutputRoot = s"$getOutput/aligned"
    set(alignmentOutput, s"$alignmentOutputRoot/recordGroupSample=$sampleId")
    set(alignmentOutputBam, s"$getOutput/aligned.bam/$sampleId.bam")
  }

  override def getOutputParams: Seq[Param[_]] = Seq(alignmentOutput)

  override def outputExists(session: SparkSession): Boolean = {
    DirectoryHelper.directoryWasCommitted(session, $(alignmentOutput))
  }

  private def loadAlignments(
      sc: SparkContext,
      input: String,
      secondInput: Option[String]): AlignmentDataset = {
    val reads = sc.loadAlignments(input, secondInput, stringency = ValidationStringency.SILENT)

    reads.transformDataset { ds =>
      import ds.sparkSession.implicits._
      ds.withColumn(
          "readName",
          split(col("readName"), " ")
            .getItem(0)
        )
        .as[AlignmentProduct]
    }
  }

  private[dnaseq] def checkBqsrParams(): Unit = {
    if (bqsrEnabled && isCustomReferenceGenome && $(knownSites).isEmpty) {
      throw new IllegalArgumentException(
        "Must specify a known sites file when using BQSR with " +
        "a custom reference genome."
      )
    }
  }

  override def execute(session: SparkSession): Unit = {
    checkBqsrParams()
    implicit val sc: SparkContext = session.sparkContext

    val SampleMetadata(sampleId, readGroups) = getSampleMetadata

    // Get the sequences from the reference build
    val sequences = sc.loadSequenceDictionary(s"file:$getReferenceGenomeDictPath")

    val alignmentsByReadGroup = readGroups.map {
      case ReadGroupMetadata(readGroupId, input, secondInput) =>
        (readGroupId, loadAlignments(session.sparkContext, input, secondInput))
    }
    val alignedFragments = runAlignmentJNI(session, sampleId, alignmentsByReadGroup, sequences)

    // run deduplication if requested
    val maybeDedupedReads = maybeDedupeReads(alignedFragments)

    // run bqsr if requested
    val maybeRecalibratedReads = maybeRecalibrateReads(session, maybeDedupedReads)

    // bin quality scores if requested
    val maybeBinnedReads = maybeBinReads(maybeRecalibratedReads)

    val previousId = maybeBinnedReads.processingSteps.lastOption.map(_.getId)
    val withProcessingSteps = maybeBinnedReads
      .replaceProcessingSteps(
        maybeBinnedReads.processingSteps ++ processingSteps(session, previousId)
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

    cleanup(session)
  }

  private def runAlignmentJNI(
      sess: SparkSession,
      sampleId: String,
      readsByGrouped: Seq[(String, AlignmentDataset)],
      sequenceDictionary: SequenceDictionary): FragmentDataset = {
    readsByGrouped.map {
      case (readGroup, alignmentDataset) =>
        val numPartitions = numAlignmentPartitions(
          sess.conf.getOption(HLSConf.ALIGNMENT_NUM_PARTITIONS.key).map(_.toInt),
          sess.conf.get(HLSConf.ALIGNMENT_MIN_PARTITIONS.key).toInt,
          alignmentDataset.rdd.getNumPartitions
        )
        val batchSize = sess.conf.get(HLSConf.BWA_MEM_JNI_BATCH_SIZE_BASES.key).toInt
        import sess.implicits._

        // Ensure that all reads for a given read name are in the same partition, then sort by
        // readInFragment so that we can pair reads
        val projectedReads = alignmentDataset
          .dataset
          .select(
            col("readName"),
            col("readInFragment"),
            encode(col("sequence"), "US-ASCII").as("sequence"),
            col("qualityScores")
          )
          .repartition(numPartitions, col("readName"))
          .sortWithinPartitions(col("readName"), col("readInFragment"))
          .as[AlignmentInput]

        val aligned = projectedReads.mapPartitions { reads =>
          val aligner = BwaAligner.defaultForSample(
            getReferenceGenomeIndexImage,
            sampleId,
            readGroup,
            $(pairedEndMode)
          )
          new AlignmentIterator(aligner, batchSize, reads)
        }

        val rgDict = ReadGroupDictionary(
          alignmentDataset.readGroups.readGroups ++ Seq(ReadGroup(sampleId, readGroup))
        )

        DatasetBoundFragmentDataset(
          aligned,
          sequenceDictionary,
          rgDict,
          alignmentDataset.processingSteps
        ): FragmentDataset
    }.reduce(_.union(_))
  }

  private def maybeDedupeReads(fragments: FragmentDataset): AlignmentDataset = {
    val maybeDeduped =
      if (getOrDefault(markDuplicates) ||
        getOrDefault(functionalEquivalence)) {
        MarkDuplicates.markDuplicateFragments(fragments)
      } else {
        fragments
      }

    ConversionUtils.fragmentsToReads(maybeDeduped)
  }

  private def maybeRecalibrateReads(
      spark: SparkSession,
      reads: AlignmentDataset): AlignmentDataset = {
    if (bqsrEnabled) {

      val knownSites = Align.getSnpTable(getKnownSites, spark)
      reads.recalibrateBaseQualities(
        spark.sparkContext.broadcast(knownSites),
        minAcceptableQuality = 6,
        optStorageLevel = Some(StorageLevel.DISK_ONLY)
      )
    } else {
      reads
    }
  }

  private def maybeBinReads(reads: AlignmentDataset): AlignmentDataset = {
    if (getOrDefault(binQualityScores) ||
      getOrDefault(functionalEquivalence)) {
      val bins = QualityScoreBin(getOrDefault(qualityScoreBinSpec))
      reads.transformDataset { ds =>
        AlignmentUtil.binQualityScores(ds, bins)
      }
    } else {
      reads
    }
  }

  override def cleanup(sess: SparkSession): Unit = {
    super.cleanup(sess)
    try {
      BwaMemIndexCache.closeAllDistributedInstances(sess.sparkContext)
    } catch {
      case NonFatal(ex) =>
        logger.warn(s"Failed closing BWA index", ex)
    }
  }

  @VisibleForTesting
  private[dnaseq] def processingSteps(
      sess: SparkSession,
      previousId: Option[String]): Seq[ProcessingStep] = {
    val versionTracker = new VersionTracker(sess, "DNASeq")
    versionTracker.addStep(
      "BWA-MEM",
      "GATK BWA-MEM JNI",
      s"refGenomeName=$getReferenceGenomeName alignPairs=${$(pairedEndMode)} " +
      s"inferPairedEndStats=true"
    )

    if ($(markDuplicates) || $(functionalEquivalence)) {
      versionTracker.addStep("Mark duplicates", "ADAM")
    }
    if (bqsrEnabled) {
      versionTracker.addStep(
        "Recalibrate base qualities",
        "ADAM",
        s"knownSites=$getKnownSites"
      )
    }
    if ($(binQualityScores) || $(functionalEquivalence)) {
      versionTracker.addStep(
        "Bin quality scores",
        "ADAM",
        s"binSpec=${getOrDefault(qualityScoreBinSpec)}"
      )
    }
    if ($(sortOnSave)) {
      versionTracker.addStep("Sort", "ADAM")
    }
    versionTracker.build(previousId)
  }

  private def bqsrEnabled: Boolean = {
    $(recalibrateBaseQualities) || $(functionalEquivalence)
  }

  /**
   * We want a number of partitions that:
   * - Is high enough so that all cores are utilized, even if we're on an autoscaling cluster at
   *   its min size
   * - Results in a relatively short task duration to avoid stragglers
   *
   * By multiplying the the number of scan partitions, which is based on input size, by a constant
   * factor, we can ensure that each alignment task is relatively short. We also enforce a minimum
   * number of tasks to ensure that the cluster is utilized.
   */
  private[dnaseq] def numAlignmentPartitions(
      confNumPartitions: Option[Int],
      confMinPartitions: Int,
      fileScanNumPartitions: Int): Int = confNumPartitions match {
    case Some(n) => n
    case None => Math.max(fileScanNumPartitions * 3, confMinPartitions)
  }
}

object Align {
  // Cache the SNP table so that we don't waste time recreating it for every sample in a
  // multisample run
  private var snpTable: SnpTable = _
  def getSnpTable(knownSitesFile: String, spark: SparkSession): SnpTable = synchronized {
    if (snpTable != null) {
      return snpTable
    }

    snpTable = FastSnpTable.fromVcf(spark, knownSitesFile)
    snpTable
  }
}
