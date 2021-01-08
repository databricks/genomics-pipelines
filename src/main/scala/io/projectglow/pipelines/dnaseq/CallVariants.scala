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

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.writer.VCFHeaderWriter
import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.spark.ml.param.Param
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.PairHMMLikelihoodCalculationEngine.PCRErrorModel
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.ReferenceConfidenceMode
import io.projectglow.common.logging._
import io.projectglow.pipelines.UserVisible
import io.projectglow.pipelines.common.{DeltaHelper, VCFSaver, VersionTracker}
import io.projectglow.pipelines.dnaseq.util.VariantCallerArgs
import io.projectglow.pipelines.UserVisible
import io.projectglow.pipelines.common.{VCFSaver, VersionTracker}
import io.projectglow.pipelines.dnaseq.assembly.{AssemblyReadsBuilder, HaplotypeShardCallerFactory, SparkAssembler}
import io.projectglow.pipelines.dnaseq.util.VariantCallerArgs

class CallVariants
    extends SingleSampleStage
    with HasAlignmentOutput
    with HasReferenceGenome
    with HasCalledVariantOutput
    with HasVcfOptions
    with HasOutput
    with HlsUsageLogging
    with HasStringency
    with HasVariantCallerParams {

  @UserVisible final val referenceConfidenceMode = new Param[String](
    this,
    "referenceConfidenceMode",
    "The reference confidence model to emit. Options are NONE " +
    "(default, emit variant sites only), BP_RESOLUTION (emit reference confidence for every " +
    "base), and GVCF (emit reference confidence for every base, but bases with similar " +
    "genotype quality are merged into contiguous bands."
  )
  setDefault(referenceConfidenceMode, "NONE")

  def setReferenceConfidenceMode(value: String): this.type = set(referenceConfidenceMode, value)

  override def init(): Unit = {
    super.init()
    val sampleId = getSampleMetadata.sample_id
    set(calledVariantOutput, s"$getCalledVariantRoot/sampleId=$sampleId")

    if ($(referenceConfidenceMode) == "NONE") {
      set(calledVariantOutputVCF, s"$getOutput/genotypes.vcf/$sampleId.vcf")
    } else {
      set(calledVariantOutputVCF, s"$getOutput/genotypes.vcf/$sampleId.g.vcf")
    }
  }

  override def outputExists(session: SparkSession): Boolean = {
    DeltaHelper.tableExists(
      session,
      getCalledVariantRoot,
      Option(("sampleId", getSampleMetadata.sample_id))
    )
  }

  override def getOutputParams: Seq[Param[_]] = Seq(calledVariantOutput)

  // Get aligned reads from Parquet if the alignment stage ran. Otherwise, get from the manifest.
  private def loadAlignments(sc: SparkContext): AlignmentDataset = {
    if (get(alignmentOutput).isDefined) {
      sc.loadAlignments($(alignmentOutput), stringency = getValidationStringency)
    } else {
      getSampleMetadata
        .read_groups
        .map {
          case ReadGroupMetadata(_, input, secondInput) =>
            sc.loadAlignments(input, secondInput, stringency = ValidationStringency.SILENT)
        }
        .reduce(_.union(_))
    }
  }

  override def execute(session: SparkSession): Unit = {

    val sc = session.sparkContext

    val optTargets = Some($(targetedRegions))
      .filter(_.nonEmpty)
      .map(sc.loadFeatures(_).cache())

    val preparedReads = loadAlignments(sc)
      .transformDataset(ds => {
        val builder = new AssemblyReadsBuilder(ds)
        optTargets
          .map(builder.filterTargetedReads)
          .getOrElse(builder)
          .filterReadsByQuality(mappingQualityThreshold = $(minMappingQuality))
          .build()
      })

    // execute Haplotype Caller on each partition
    val factory = new HaplotypeShardCallerFactory(getReferenceGenomeFastaPath, genVcArgs())
    val variantRows = SparkAssembler.shardAndCallVariants(
      preparedReads,
      factory,
      optTargets
    )

    val vcfHeader = factory.getOutputHeader(preparedReads)
    vcfHeader.addMetaDataLine(vcfHeaderProcessingStep)

    VCFSaver.saveToDeltaAndMaybeVCF(
      VCFHeaderWriter.writeHeaderAsString(vcfHeader),
      variantRows.withColumn("sampleId", lit(getSampleMetadata.sample_id)),
      $(exportVCF),
      $(exportVCFAsSingleFile),
      getCalledVariantRoot,
      Some(("sampleId", Set(getSampleMetadata.sample_id))),
      $(calledVariantOutputVCF),
      $(vcfCompressionCodec)
    )
  }

  private def vcfHeaderProcessingStep: VCFHeaderLine = {
    val gatkVersion = VersionTracker.toolVersion("GATK")
    val params = Seq(
      s"variantCaller=GATK$gatkVersion/HaplotypeCaller",
      s"refGenomeName=$getReferenceGenomeName",
      s"referenceConfidenceMode=${$(referenceConfidenceMode)}",
      s"pcrIndelModel=${$(pcrIndelModel)}",
      s"targetedRegions=${$(targetedRegions)}",
      s"maxReadStartsPerPosition=${$(maxReadStartsPerPosition)}"
    )
    new VCFHeaderLine("DatabricksParams", params.mkString(" "))
  }

  private def genVcArgs(): VariantCallerArgs = {
    VariantCallerArgs(
      ReferenceConfidenceMode.valueOf($(referenceConfidenceMode)),
      PCRErrorModel.valueOf($(pcrIndelModel)),
      Some($(maxReadStartsPerPosition)).filter(_ > 0)
    )
  }
}
