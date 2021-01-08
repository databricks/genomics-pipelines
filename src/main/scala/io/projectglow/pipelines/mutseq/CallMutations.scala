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

package io.projectglow.pipelines.mutseq

import htsjdk.variant.variantcontext.writer.VCFHeaderWriter
import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.PairHMMLikelihoodCalculationEngine.PCRErrorModel
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.ReferenceConfidenceMode
import io.projectglow.common.logging._
import org.bdgenomics.adam.models.ReadGroupDictionary
import io.projectglow.pipelines.UserVisible
import io.projectglow.pipelines.common.{DeltaHelper, VCFSaver, VersionTracker}
import io.projectglow.pipelines.dnaseq.assembly.{AssemblyReadsBuilder, Mutect2ShardCallerFactory, SparkAssembler}
import io.projectglow.pipelines.dnaseq.util.VariantCallerArgs
import io.projectglow.pipelines.UserVisible
import io.projectglow.pipelines.common.{VCFSaver, VersionTracker}
import io.projectglow.pipelines.dnaseq.{HasCalledVariantOutput, HasOutput, HasReferenceGenome, HasStringency, HasVariantCallerParams, HasVcfOptions}
import io.projectglow.pipelines.dnaseq.assembly.{AssemblyReadsBuilder, Mutect2ShardCallerFactory, SparkAssembler}
import io.projectglow.pipelines.dnaseq.util.VariantCallerArgs

class CallMutations
    extends GroupedSampleStage
    with HasReferenceGenome
    with HasCalledVariantOutput
    with HasVcfOptions
    with HasOutput
    with HlsUsageLogging
    with HasStringency
    with HasVariantCallerParams {

  final val normalAlignmentOutput =
    new Param[String](this, "alignmentOutputNormal", "Aligned reads for normal sample")

  def setNormalAlignmentOutput(value: String): this.type = set(normalAlignmentOutput, value)

  final val tumorAlignmentOutput =
    new Param[String](this, "alignmentOutputTumor", "Aligned reads for tumor sample")

  def setTumorAlignmentOutput(value: String): this.type = set(tumorAlignmentOutput, value)

  @UserVisible
  final val panelOfNormals = new Param[String](
    this,
    "panelOfNormals",
    "A VCF file or directory of VCF files with sites observed in normal samples. Sites that " +
    "appear in the panel of normals will be filtered from the output."
  )
  setDefault(panelOfNormals, "")
  def setPanelOfNormals(value: String): this.type = set(panelOfNormals, value)

  override def init(): Unit = {
    super.init()
    val sampleId = getGroupedSampleMetadata.pair_id
    set(calledVariantOutput, s"$getCalledMutationRoot/sampleId=$sampleId")
    set(calledVariantOutputVCF, s"$getOutput/mutations.vcf/$sampleId.vcf")
  }

  override def getOutputParams: Seq[Param[_]] = Seq(calledVariantOutput)

  override def outputExists(session: SparkSession): Boolean = {
    DeltaHelper.tableExists(
      session,
      getCalledMutationRoot,
      Option(("sampleId", getGroupedSampleMetadata.pair_id))
    )
  }

  override def execute(session: SparkSession): Unit = {

    val sc = session.sparkContext

    val optTargets = Some($(targetedRegions))
      .filter(_.nonEmpty)
      .map(sc.loadFeatures(_).cache())

    val optExclusions = Some($(excludedRegions))
      .filter(_.nonEmpty)
      .map(sc.loadFeatures(_).cache())

    val tumorAlignments =
      sc.loadAlignments($(tumorAlignmentOutput), stringency = getValidationStringency)
    val normalAlignments = get(normalAlignmentOutput).map { path =>
      sc.loadAlignments(path, stringency = getValidationStringency)
    }.toSeq
    val allReads = tumorAlignments.union(normalAlignments: _*)

    val preparedReads = allReads.transformDataset { ds =>
      val builder = new AssemblyReadsBuilder(ds)
      optTargets
        .map(builder.filterTargetedReads)
        .getOrElse(builder)
        .filterReadsByQuality(mappingQualityThreshold = $(minMappingQuality))
        .build()
    }

    // get sample metadata and extract tumor/normal sample ids
    val samples = getGroupedSampleMetadata
    val tumor = samples
      .samples
      .find(_._1 == "tumor")
      .getOrElse({
        throw new IllegalArgumentException(s"Did not find tumor sample in $samples.")
      })
      ._2
    val optNormal = samples.samples.find(_._1 == "normal").map(_._2)

    // execute Mutect 2 on each partition
    val factory = new Mutect2ShardCallerFactory(
      getReferenceGenomeFastaPath,
      genVcArgs(),
      optNormal.map(_.sample_id)
    )

    // Only use read groups for the provided tumor and normal sample IDs; these define the SAM
    // header used by Mutect2
    val tnSamples = Seq(tumor.sample_id) ++ optNormal.map(_.sample_id).toSeq
    val tnReadGroups = preparedReads.readGroups.readGroups.filter { rg =>
      tnSamples.contains(rg.sampleId)
    }
    val preparedReadsWithTNSamples =
      preparedReads.replaceReadGroups(ReadGroupDictionary(tnReadGroups))

    val variantRows = SparkAssembler.shardAndCallVariants(
      preparedReadsWithTNSamples,
      factory,
      optTargets,
      optExclusions
    )
    val filteredVariantRows = filterByPanelOfNormals(variantRows)

    val vcfHeader = factory.getOutputHeader(preparedReads)
    vcfHeader.addMetaDataLine(vcfHeaderProcessingStep)

    VCFSaver.saveToDeltaAndMaybeVCF(
      VCFHeaderWriter.writeHeaderAsString(vcfHeader),
      filteredVariantRows.withColumn("sampleId", lit(getGroupedSampleMetadata.pair_id)),
      $(exportVCF),
      $(exportVCFAsSingleFile),
      getCalledMutationRoot,
      Some(("sampleId", Set(getGroupedSampleMetadata.pair_id))),
      $(calledVariantOutputVCF),
      $(vcfCompressionCodec)
    )
  }

  private def vcfHeaderProcessingStep: VCFHeaderLine = {
    val gatkVersion = VersionTracker.toolVersion("GATK")
    val params = Seq(
      s"variantCaller=GATK$gatkVersion/Mutect2",
      s"refGenomeName=$getReferenceGenomeName",
      s"pcrIndelModel=${$(pcrIndelModel)}",
      s"targetedRegions=${$(targetedRegions)}",
      s"excludedRegions=${$(excludedRegions)}",
      s"maxReadStartsPerPosition=${$(maxReadStartsPerPosition)}"
    )
    new VCFHeaderLine("DatabricksParams", params.mkString(" "))
  }

  private def genVcArgs(): VariantCallerArgs = {
    VariantCallerArgs(
      ReferenceConfidenceMode.NONE, // MuTect2 does not support GVCF
      PCRErrorModel.valueOf($(pcrIndelModel)),
      Some($(maxReadStartsPerPosition)).filter(_ > 0)
    )
  }

  private def filterByPanelOfNormals(df: DataFrame): DataFrame = {
    if ($(panelOfNormals).isEmpty) {
      return df
    }

    val sess = df.sparkSession

    val ponSites = sess
      .read
      .format("vcf")
      .load($(panelOfNormals))

    df.join(
      ponSites,
      df("contigName") === ponSites("contigName") &&
      df("start") === ponSites("start"),
      "left_anti"
    )
  }
}
