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

import java.nio.file.Paths
import java.util.NoSuchElementException

import htsjdk.samtools.ValidationStringency
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.PrivateUtils
import org.apache.spark.ml.param.{BooleanParam, IntParam, Param, Params}
import org.apache.spark.ml.util.Identifiable
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.HaplotypeCaller
import io.projectglow.pipelines.UserVisible
import io.projectglow.pipelines.common.VCFSaver

trait HasDefaultUid extends Identifiable {
  override val uid = Identifiable.randomUID(this.getClass.getName)
}

trait HasReplayMode extends Params {
  @UserVisible final val replayMode = new Param[String](
    this,
    "replayMode",
    "How to handle cases where the pipeline output already exists. If replayMode is " +
    "'overwrite', the existing output will be deleted. If the replayMode is 'skip',  stages " +
    "with existing output will be skipped."
  )
  setDefault(replayMode, "skip")

  def getReplayMode: PipelineReplayMode = PipelineReplayMode.fromString($(replayMode))
}

sealed trait PipelineReplayMode
object PipelineReplayMode {
  case object Overwrite extends PipelineReplayMode
  case object Skip extends PipelineReplayMode

  def fromString(mode: String): PipelineReplayMode = mode.toLowerCase match {
    case "overwrite" => Overwrite
    case "skip" => Skip
    case m =>
      throw new IllegalArgumentException(
        s"Invalid replay mode $m. Replay mode " +
        s"must be 'overwrite' or 'skip'."
      )
  }
}

trait HasOutput extends Params {

  @UserVisible final val output =
    new Param[String](this, "output", "Output Path for storing pipeline results")

  def setOutput(value: String): this.type = set(output, value)

  def getOutput: String = {
    // strips suffix to ensure downstream callers can use this without worrying about
    // stripping out the /
    // stripping suffix is essential for any code that uses this file path as key in uploading to S3
    // S3 multipart uploads will silently fail for keys of the form $bucket/a//b
    $(output).stripSuffix("/")
  }
}

trait HasOutputSuffix {
  def outputSuffix: String = ""
}

trait HasBamOutput extends Params {
  @UserVisible final val exportBam = new BooleanParam(
    this,
    "exportBam",
    "Export BAM Format (True/False)"
  )

  @UserVisible final val exportBamAsSingleFile = new BooleanParam(
    this,
    "exportBamAsSingleFile",
    "If this parameter is true, the exported BAM will be condensed to a single file. " +
    "If false, the exported BAM will be split into several files, each of which will have " +
    "a valid header."
  )

  final val sortOnSave = new BooleanParam(
    this,
    "sortOnSave",
    "If true, sorts the reads by coordinate when saving to BAM. " +
    "If false, outputs unsorted reads grouped by queryname."
  )

  final val alignmentOutputBam =
    new Param[String](this, "bamOutput", "Output of Aligned Reads (BAM)")

  setDefault(exportBam, false)
  setDefault(exportBamAsSingleFile, true)
  setDefault(sortOnSave, true)

  def setExportBam(value: Boolean): this.type = set(exportBam, value)

  def setExportBamAsSingleFile(value: Boolean): this.type = set(exportBamAsSingleFile, value)

  def setSortOnSave(value: Boolean): this.type = set(sortOnSave, value)

  def setAlignmentOutputBam(value: String): this.type = set(alignmentOutputBam, value)

  def getExportBam: Boolean = $(exportBam)

  def getExportBamAsSingleFile: Boolean = $(exportBamAsSingleFile)

  def getSortOnSave: Boolean = $(sortOnSave)

  def getAlignmentOutputBam: String = $(alignmentOutputBam)
}

trait HasAlignmentOutput extends Params with HasOutputSuffix {

  final val alignmentOutput =
    new Param[String](this, "alignmentOutput" + outputSuffix, "Output of Aligned Reads (Parquet)")

  def setAlignmentOutput(value: String): this.type = set(alignmentOutput, value)

  def getAlignmentOutput: String = $(alignmentOutput).stripSuffix("/")
}

trait HasDBNucleusHome extends Params {

  final val home =
    new Param[String](this, "DB Nucleus Home", "DB Nucleus Home dir on the container")

  setDefault(home, System.getenv("DBNUCLEUS_HOME"))

  def getHome: String = $(home).stripSuffix("/")
  def getGenomicsHome: String = s"$getHome/dbgenomics"

  def setHome(value: String): this.type = set(home, value)
}

trait HasReferenceGenome extends HasDBNucleusHome {

  // This section of the file handles configuration for reference genome bundles built by
  // Databricks. Pipelines that should work with custom reference genomes should NOT directly
  // access any of these parameters.
  final val GRCH37 = "grch37"
  final val GRCH37Star = "grch37_star"
  final val GRCH37EnsemblVep = "grch37_vep_96"
  final val GRCH37RefSeqVep = "grch37_refseq_vep_96"
  final val GRCH37MergedVep = "grch37_merged_vep_96"
  final val GRCH38 = "grch38"
  final val GRCH38Star = "grch38_star"
  final val GRCH38EnsemblVep = "grch38_vep_96"
  final val GRCH38RefSeqVep = "grch38_refseq_vep_96"
  final val GRCH38MergedVep = "grch38_merged_vep_96"

  // These params necessary to figure out the layout of the prebuilt reference genomes
  final val refGenomeId = new Param[String](this, "refGenomeId", "Reference Genome ID")
  setDefault(refGenomeId, sys.env.getOrElse(refGenomeId.name, ""))
  final val refGenomePath = new Param[String](this, "refGenomePath", "Reference Genome Path")
  final val refGenomeName = new Param[String](this, "refGenomeName", "Reference Genome Name")

  final val refGenomeIdToName = Map(
    GRCH37 -> "human_g1k_v37",
    GRCH37Star -> "human_g1k_v37",
    GRCH37EnsemblVep -> "human_g1k_v37",
    GRCH37RefSeqVep -> "human_g1k_v37",
    GRCH37MergedVep -> "human_g1k_v37",
    GRCH38 -> "GRCh38_full_analysis_set_plus_decoy_hla",
    GRCH38Star -> "GRCh38_full_analysis_set_plus_decoy_hla",
    GRCH38EnsemblVep -> "GRCh38_full_analysis_set_plus_decoy_hla",
    GRCH38RefSeqVep -> "GRCh38_full_analysis_set_plus_decoy_hla",
    GRCH38MergedVep -> "GRCh38_full_analysis_set_plus_decoy_hla"
  )

  def getReferenceGenomeId: String = $(refGenomeId)

  def setReferenceGenomeId(value: String): this.type = set(refGenomeId, value)

  def getPrebuiltReferenceGenomeName: String =
    get(refGenomeName).getOrElse(
      refGenomeIdToName.getOrElse(
        getReferenceGenomeId, {
          throw new NoSuchElementException(
            s"Could not find reference genome id to name mapping " +
            s"for refGenomeId=$getReferenceGenomeId. Did you enter an incorrect refGenomeId " +
            "environment variable, or forget to set the refGenomeName environment variable?"
          )
        }
      )
    )

  def getPrebuiltReferenceGenomePath: String =
    get(refGenomePath).getOrElse(s"$getGenomicsHome/$getReferenceGenomeId")

  def getPrebuiltReferenceFastaPath: String = {
    "%s/data/%s.fa".format(getPrebuiltReferenceGenomePath, getPrebuiltReferenceGenomeName)
  }

  // The following parameters are valid for either custom reference genomes or those prebuilt
  // by Databricks. Prefer accessing file resources through these methods.
  final lazy val CUSTOM_REFERENCE_DIR = s"$getGenomicsHome/reference-genome"
  final val CUSTOM_REFERENCE_ENV_VAR = "REF_GENOME_PATH"

  final val referenceGenomeFastaPath =
    new Param[String](this, "referenceGenomeFastaPath", "reference genome fasta path")

  def isCustomReferenceGenome: Boolean = sys.env.contains(CUSTOM_REFERENCE_ENV_VAR)

  def getReferenceGenomeFastaPath: String =
    get(referenceGenomeFastaPath)
      .orElse(sys.env.get(CUSTOM_REFERENCE_ENV_VAR).map { p =>
        val fastaFileName = Paths.get(p).getFileName
        Paths.get(CUSTOM_REFERENCE_DIR).resolve(fastaFileName).toString
      })
      .getOrElse(getPrebuiltReferenceFastaPath)

  def setReferenceGenomeFastaPath(value: String): this.type = set(referenceGenomeFastaPath, value)

  def getReferenceGenomeName: String = {
    Paths
      .get(getReferenceGenomeFastaPath)
      .getFileName
      .toString
      .stripSuffix(".fasta")
      .stripSuffix(".fa")
  }

  def getReferenceGenomeIndexImage: String = getReferenceGenomeFastaPath + ".img"
  def getReferenceGenomeDictPath: String =
    FilenameUtils.removeExtension(getReferenceGenomeFastaPath) + ".dict"
}

trait HasVcfOutput extends HasVcfOptions {

  final val calledVariantOutputVCF =
    new Param[String](this, "callVariantOutputVCF", "Output of Called Variants (VCF)")

  def setCalledVariantOutputVCF(value: String): this.type = set(calledVariantOutputVCF, value)

  def getCalledVariantOutputVCF(conf: Configuration): String = {
    getVCFPath($(calledVariantOutputVCF), conf)
  }
}

trait HasCalledVariantOutput extends HasVcfOutput with HasOutput {

  @UserVisible final val exportVCF =
    new BooleanParam(this, "exportVCF", "Export VCF Format (True/False)")
  setDefault(exportVCF, false)

  final val calledVariantOutput =
    new Param[String](this, "callVariantOutput", "Output of Called Variants (Delta)")

  def setCalledVariantOutput(value: String): this.type = set(calledVariantOutput, value)
  def setExportVCF(value: Boolean): this.type = set(exportVCF, value)

  def getCalledVariantRoot: String = s"$getOutput/genotypes"
  def getCalledMutationRoot: String = s"$getOutput/mutations"
  def getCalledVariantOutput: String = $(calledVariantOutput).stripSuffix("/")
}

trait HasVcfOptions extends Params {
  @UserVisible final val exportVCFAsSingleFile = new BooleanParam(
    this,
    "exportVCFAsSingleFile",
    "If this parameter is true, the exported VCF will be condensed to a single file. " +
    "If false, the exported VCF will be split into several files, each of which will have " +
    "a valid header."
  )
  setDefault(exportVCFAsSingleFile, true)

  @UserVisible final val vcfCompressionCodec = new Param[String](
    this,
    "vcfCompressionCodec",
    "If set, the exported VCF will be compressed with the specified codec. " +
    "Options are bgzf (default), gzip, or empty to save an uncompressed VCF file."
  )
  setDefault(vcfCompressionCodec, "bgzf")

  def setExportVCFAsSingleFile(value: Boolean): this.type = set(exportVCFAsSingleFile, value)
  def setVcfCompressionCodec(value: String): this.type = set(vcfCompressionCodec, value)

  protected def getVCFPath(basePath: String, hadoopConf: Configuration): String = {
    if ($(exportVCFAsSingleFile)) {
      VCFSaver.compressedVcfFileName(hadoopConf, $(vcfCompressionCodec), basePath)
    } else {
      basePath
    }
  }
}

trait HasStringency extends Params {

  @UserVisible final val validationStringency = new Param[String](
    this,
    "validationStringency",
    "Validation Stringency when loading from input files. " +
    "If STRICT, throws an exception on invalid records. " +
    "If LENIENT, logs invalid records to standard error, and discards the record. " +
    "If SILENT, discards invalid records without logging."
  )
  setDefault(validationStringency, ValidationStringency.STRICT.toString)

  def getValidationStringency: ValidationStringency =
    ValidationStringency.valueOf($(validationStringency))

  def setValidationStringency(value: ValidationStringency): this.type =
    set(validationStringency, value.toString)
}

trait HasVcfValidation extends Params {
  @UserVisible final val performValidation = new BooleanParam(
    this,
    "performValidation",
    "If true, validates that input gVCF records have the necessary information for joint genotyping."
  )
  setDefault(performValidation, false)

  def getPerformValidation: Boolean = $(performValidation)

  def setPerformValidation(value: Boolean): this.type = set(performValidation, value)
}

trait HasTargetedRegionsParams extends Params {
  @UserVisible final val targetedRegions = new Param[String](
    this,
    "targetedRegions",
    "Path to BED6/12, GFF3, GTF/GFF2, NarrowPeak, or IntervalList files containing regions " +
    "to call. If omitted, calls all regions."
  )
  setDefault(targetedRegions, "")
  def setTargetedRegions(value: String): this.type = set(targetedRegions, value)
}

trait HasVariantCallerParams extends HasTargetedRegionsParams {
  @UserVisible final val minMappingQuality = new IntParam(
    this,
    "minMappingQuality",
    "Reads with mapping quality below this value will be removed before variant calling."
  )
  setDefault(minMappingQuality, 20)

  @UserVisible final val maxReadStartsPerPosition = new IntParam(
    this,
    "maxReadStartsPerPosition",
    "An optional threshold at which to downsample reads at a reference position where " +
    "many reads align. Setting this parameter will reduce the amount of time spent calling " +
    "variants in regions with pathologically high coverage. If this value is set to be " +
    "negative, then downsampling will not be performed. The default is set at " +
    s"${HaplotypeCaller.DEFAULT_MAX_READS_PER_ALIGNMENT} to match GATK's default settings."
  )
  setDefault(
    maxReadStartsPerPosition,
    HaplotypeCaller.DEFAULT_MAX_READS_PER_ALIGNMENT
  )
  def setMaxReadsPerPosition(value: Int): this.type = set(maxReadStartsPerPosition, value)

  @UserVisible final val pcrIndelModel = new Param[String](
    this,
    "pcrIndelModel",
    "How aggressively HaplotypeCaller should attempt to correct for PCR errors when " +
    "calling indels. When dealing with PCR-free input data, set this parameter to NONE. Other " +
    "options are CONSERVATIVE (default), AGGRESSIVE, and HOSTILE"
  )
  setDefault(pcrIndelModel, "CONSERVATIVE")

  @UserVisible final val excludedRegions =
    new Param[String](this, "excludedRegions", "Regions to exclude. If omitted, calls all regions.")
  setDefault(excludedRegions, "")
  def setExcludedRegions(value: String): this.type = set(excludedRegions, value)
}

trait HasSampleMetadata extends Params {

  final val sampleMetadata = new Param[SampleMetadata](this, "sample_metadata", "Sample Metadata")

  def setSampleMetadata(value: SampleMetadata): this.type = set(sampleMetadata, value)

  def getSampleMetadata: SampleMetadata = $(sampleMetadata)
}

trait HasManifest extends Params {

  @UserVisible final val manifest =
    new Param[String](this, "manifest", "Manifest File Path for FASTQ(BAM) Reads")

  def setManifest(value: String): this.type = set(manifest, value)
}

trait HasPerSampleTimeout extends Params {
  import scala.concurrent.duration._
  @UserVisible final val perSampleTimeout =
    new Param[String](
      this,
      "perSampleTimeout",
      "Timeout applied per sample. After hitting this timeout, the pipeline will " +
      "continue on to the next sample. The value of this parameter must include a time unit: 's' " +
      "for seconds, 'm' for minutes, or 'h' for hours. For example, '60m' will result in a " +
      "timeout of 60 minutes."
    )
  setDefault(perSampleTimeout, "12h")
  def setPerSampleTimeout(timeout: String): this.type = set(perSampleTimeout, timeout)
  def getPerSampleTimeout: FiniteDuration =
    PrivateUtils.utils.timeStringAsMs($(perSampleTimeout)).millis
}

trait HasVcfManifest extends Params {

  @UserVisible final val vcfManifest =
    new Param[String](
      this,
      "manifest",
      "Manifest File Path for VCFs. Required unless starting from Delta."
    )

  setDefault(vcfManifest, "")

  def hasVcfManifest: Boolean = $(vcfManifest).nonEmpty

  def setVcfManifest(value: String): this.type = set(vcfManifest, value)
}

trait HasGvcfDeltaOutput extends Params {

  @UserVisible final val gvcfDeltaOutput =
    new Param[String](
      this,
      "gvcfDeltaOutput",
      "Delta output path for input GVCFs. If empty, does not export GVCFs to Delta."
    )

  @UserVisible final val saveMode =
    new Param[String](
      this,
      "saveMode",
      "Table save behavior: append, overwrite, errorIfExists, or ignore"
    )

  setDefault(gvcfDeltaOutput, "")
  setDefault(saveMode, "overwrite")

  def exportGVCFToDelta: Boolean = $(gvcfDeltaOutput).nonEmpty

  def setGvcfDeltaOutput(value: String): this.type = set(gvcfDeltaOutput, value)
  def setSaveMode(value: String): this.type = set(saveMode, value)

  def getGvcfDeltaOutput: String = $(gvcfDeltaOutput)
}

trait HasInputVcf extends Params {

  @UserVisible final val inputVcf =
    new Param[String](this, "inputVcf", "Input VCF File Path")

  def setInputVcf(value: String): this.type = set(inputVcf, value)
}

trait HasInputVariants extends Params {

  @UserVisible final val inputVariants =
    new Param[String](this, "inputVariants", "Input File Path for Variants (VCF or Delta)")

  def setInputVariants(value: String): this.type = set(inputVariants, value)
}

trait HasSnpEffParams extends HasVariantAnnotaterParams with HasReferenceGenome {

  final val snpEffDB = new Param[String](this, "snpEffDB", "The Database we are interested in")

  final val snpEffPath =
    new Param[String](this, "snpEff", "Path where the snpEff database is located")

  final val refGenomeIdToSnpEffDB = Map(
    GRCH37 -> "GRCh37.75",
    GRCH38 -> "GRCh38.86"
  )

  def getSnpEffDB: String = {
    val snpEffDBOpt = get(snpEffDB).orElse(refGenomeIdToSnpEffDB.get($(refGenomeId)))
    if (snpEffDBOpt.isEmpty) {
      throw new NoSuchElementException(
        s"Could not find reference genome id to snpEff database mapping " +
        s"for refGenomeId=$refGenomeId. Did you enter an incorrect refGenomeId " +
        "environment variable, or forget to set the refGenomeName environment variable?"
      )
    } else {
      snpEffDBOpt.get
    }
  }

  def getSnpEffPath: String =
    get(snpEffPath)
      .getOrElse(s"$getGenomicsHome/$getReferenceGenomeId/snpEff")

  def setSnpEffDB(value: String): this.type = set(snpEffDB, value)

  def setSnpEffPath(value: String): this.type = set(snpEffPath, value)
}

trait HasVepParams extends HasVariantAnnotaterParams with HasReferenceGenome {

  final val vepCache =
    new Param[String](this, "vepCache", "Path where the cached VEP database is located")

  final val vepCmd =
    new Param[String](this, "vepCmd", "Path where the VEP commmand line tool is located")

  @UserVisible final val extraVepOptions =
    new Param[String](
      this,
      "extraVepOptions",
      "Additional command line options to pass to VEP. Some options are set by the pipeline " +
      "and cannot be overridden: --assembly, --cache, --dir_cache, --fasta, --format, --merged, " +
      "--no_stats, --offline, --output_file, --refseq, --vcf."
    )
  setDefault(extraVepOptions, "--everything --minimal --allele_number --fork 4")

  final val refGenomeIdToVepAssembly = Map(
    GRCH37EnsemblVep -> "GRCh37",
    GRCH37RefSeqVep -> "GRCh37",
    GRCH37MergedVep -> "GRCh37",
    GRCH38EnsemblVep -> "GRCh38",
    GRCH38RefSeqVep -> "GRCh38",
    GRCH38MergedVep -> "GRCh38"
  )

  final val refGenomeIdToVepTranscripts = Map(
    GRCH37EnsemblVep -> None,
    GRCH37RefSeqVep -> Some("--refseq"),
    GRCH37MergedVep -> Some("--merged"),
    GRCH38EnsemblVep -> None,
    GRCH38RefSeqVep -> Some("--refseq"),
    GRCH38MergedVep -> Some("--merged")
  )

  def getVepAssembly: String = {
    val assemblyOpt = refGenomeIdToVepAssembly.get($(refGenomeId))
    if (assemblyOpt.isEmpty) {
      throw new NoSuchElementException(
        s"Could not find reference genome id to VEP assembly mapping " +
        s"for refGenomeId=${$(refGenomeId)}. Did you enter an incorrect refGenomeId " +
        "environment variable, or forget to set the vepAssembly environment variable?"
      )
    } else {
      assemblyOpt.get
    }
  }

  def getVepTranscripts: Option[String] = {
    if (!refGenomeIdToVepTranscripts.contains($(refGenomeId))) {
      throw new NoSuchElementException(
        s"Could not find reference genome id to VEP transcripts mapping " +
        s"for refGenomeId=${$(refGenomeId)}. Did you enter an incorrect refGenomeId " +
        "environment variable, or forget to set the vepTranscripts environment variable?"
      )
    }
    refGenomeIdToVepTranscripts($(refGenomeId))
  }

  def getVepCmd: String = get(vepCmd).getOrElse(s"/opt/vep/src/ensembl-vep/vep")

  def getExtraVepOptions: Seq[String] = $(extraVepOptions).split(" ")

  def getVepScript: Seq[String] = {
    val baseVepOpts = Seq(
      "--no_stats",
      "--cache",
      "--offline",
      "--format", // Input format
      "vcf",
      "--output_file",
      "STDOUT",
      "--vcf" // Output format
    )
    Seq(
      getVepCmd,
      "--dir_cache",
      getPrebuiltReferenceGenomePath,
      "--fasta",
      getReferenceGenomeFastaPath,
      "--assembly",
      getVepAssembly
    ) ++ baseVepOpts ++ getVepTranscripts ++ getExtraVepOptions
  }
}

trait HasVariantAnnotaterParams extends HasVcfOptions {

  final val annotationOutput =
    new Param[String](this, "annotationOutput", "Output path for annotated variants")

  final val annotationOutputVCF =
    new Param[String](this, "annotationOutputVCF", "Output path for annotated variants (VCF)")

  def getAnnotationOutput: String = $(annotationOutput)

  def getAnnotationOutputVCF(conf: Configuration): String = {
    getVCFPath($(annotationOutputVCF), conf)
  }

  def setAnnotationOutputVCF(path: String): this.type = set(annotationOutputVCF, path)
}
