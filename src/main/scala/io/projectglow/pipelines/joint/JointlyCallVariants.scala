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

package io.projectglow.pipelines.joint

import java.io.ByteArrayOutputStream
import java.nio.file.Paths
import java.util.Collections

import scala.collection.JavaConverters._
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.writer.{VCFHeaderWriter, VariantContextWriterBuilder}
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}
import io.projectglow.vcf.{InternalRowToVariantContextConverter, VCFHeaderUtils, VCFSchemaInferrer, VariantContextToInternalRowConverter}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.hls.dsl.expressions.bin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SQLUtils, SparkSession}
import org.broadinstitute.hellbender.cmdline.argumentcollections.DbsnpArgumentCollection
import org.broadinstitute.hellbender.engine.ReferenceDataSource
import org.broadinstitute.hellbender.tools.walkers.{GenotypeGVCFsEngine, ReferenceConfidenceVariantContextMerger}
import org.broadinstitute.hellbender.tools.walkers.annotator.{Annotation, RMSMappingQuality, StandardAnnotation, VariantAnnotatorEngine}
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeCalculationArgumentCollection
import io.projectglow.pipelines.common.{DeltaHelper, VCFSaver, VersionTracker}
import io.projectglow.pipelines.dnaseq.{HasCalledVariantOutput, HasGvcfDeltaOutput, HasOutput, HasReferenceGenome, HasStringency, HasTargetedRegionsParams, HasVcfManifest, HasVcfOptions, HasVcfValidation, Manifest}
import io.projectglow.pipelines.PipelineStage
import io.projectglow.pipelines.{dnaseq, PipelineStage}
import io.projectglow.pipelines.common.{VCFSaver, VersionTracker}
import io.projectglow.pipelines.dnaseq.{HasCalledVariantOutput, HasGvcfDeltaOutput, HasOutput, HasReferenceGenome, HasStringency, HasTargetedRegionsParams, HasVcfManifest, HasVcfOptions, HasVcfValidation}
import io.projectglow.pipelines.dnaseq.assembly.AssemblyShardCaller
import io.projectglow.pipelines.sql.HLSConf

class JointlyCallVariants
    extends PipelineStage
    with HasCalledVariantOutput
    with HasVcfOptions
    with HasGvcfDeltaOutput
    with HasOutput
    with HasReferenceGenome
    with HasStringency
    with HasTargetedRegionsParams
    with HasVcfManifest
    with HasVcfValidation {

  override def init(): Unit = {
    super.init()
    set(calledVariantOutput, s"$getOutput/genotypes")
    set(calledVariantOutputVCF, s"$getOutput/genotypes.vcf")
  }

  override def outputExists(session: SparkSession): Boolean = {
    DeltaHelper.tableExists(session, getCalledVariantOutput, None)
  }

  override def getOutputParams: Seq[Param[_]] = Seq(calledVariantOutput)

  /**
   * Loads variant contexts from Delta, squares them off, bins them so that overlapping variant
   * contexts are in the same partition, generates joint variant calls, and prints to Parquet
   * and/or VCF.
   */
  override def execute(session: SparkSession): Unit = {
    // The default number of shuffle partitions is 200, but this causes us to bottle-neck on
    // shuffle-heavy operations.
    // If the Spark conf is not set, we use the number of CPUs available (x3).
    val numShufflePartitions = SQLConf
      .get
      .getConf(HLSConf.JOINT_GENOTYPING_NUM_SHUFFLE_PARTITIONS)
    SQLConf.get.setConf(SQLConf.SHUFFLE_PARTITIONS, numShufflePartitions)

    val (header, variantRows) = getHeaderAndVariantRows(session)
    val binnedVariantRows = binVariantRows(variantRows)
    val jointCalls = generateJointCalls(header, binnedVariantRows)

    VCFSaver.saveToDeltaAndMaybeVCF(
      VCFHeaderWriter.writeHeaderAsString(header),
      jointCalls,
      $(exportVCF),
      $(exportVCFAsSingleFile),
      $(calledVariantOutput),
      None,
      $(calledVariantOutputVCF),
      $(vcfCompressionCodec)
    )
  }

  /**
   * Creates a VCF header based on the manifest, and a DataFrame of variant rows from either Delta
   * or the VCF paths in the manifest.
   */
  private[joint] def getHeaderAndVariantRows(session: SparkSession): (VCFHeader, DataFrame) = {
    val (baseHeader, filteredVariantRowDf) = (exportGVCFToDelta, hasVcfManifest) match {
      case (true, true) =>
        val inputPathDs = dnaseq.Manifest.loadFilePaths($(vcfManifest))(session.sqlContext)
        val header = IngestVariants.createVcfHeader(session, inputPathDs.collect())
        val df = session.read.format("delta").load($(gvcfDeltaOutput))
        (header, df)
      case (true, false) =>
        import session.implicits._
        val df = session.read.format("delta").load($(gvcfDeltaOutput))
        val headerLines = VCFSchemaInferrer.headerLinesFromSchema(df.schema)
        val sampleIds = df
          .selectExpr("genotypes[0].sampleId as sampleId")
          .distinct()
          .orderBy("sampleId") // The GATK orders samples alphabetically
          .as[String]
          .collectAsList
        val header = new VCFHeader(headerLines.toSet.asJava, sampleIds)
        (header, df)
      case (false, true) =>
        val inputPathDs = dnaseq.Manifest.loadFilePaths($(vcfManifest))(session.sqlContext)
        val header = IngestVariants.createVcfHeader(session, inputPathDs.collect())
        val df = IngestVariants.readAndFilter(
          inputPathDs,
          // Infer schema here so that we don't have to read VCF headers twice
          Some(VCFSchemaInferrer.inferSchema(true, true, header)),
          $(targetedRegions),
          $(performValidation),
          getValidationStringency
        )
        (header, df)
      case _ => throw new IllegalArgumentException("Must provide a GVCF manifest or Delta table.")
    }

    // Add the header lines for the GenotypeGVCFs tool
    val outputHeader = JointlyCallVariants.getGenotypeGvcfsEngineHeader(
      baseHeader,
      getReferenceGenomeName
    )
    (outputHeader, filteredVariantRowDf)
  }

  /**
   * GenotypingEngine (called by GenotypeGVCFs) is stateful and assumes it has sees all the variant
   * contexts sequentially. In doing so, it determines if a spanning deletion is spurious based on
   * earlier indels (maintained in a list upstreamDeletionsLoc). We ensure that the list of relevant
   * indels is in a correct state by placing all overlapping VCs in the same bin. To avoid
   * outputting the same VC twice, we tag the duplicate (via isDuplicate) to mark it for removal
   * downstream (in generateJointCalls).
   *
   * @return DataFrame sorted with overlapping VCs in the same partition
   */
  private[joint] def binVariantRows(variantRows: DataFrame): DataFrame = {

    val binSize = SQLConf.get.getConf(HLSConf.JOINT_GENOTYPING_BIN_SIZE)
    val numBinPartitions = SQLConf.get.getConf(HLSConf.JOINT_GENOTYPING_NUM_BIN_PARTITIONS)

    import variantRows.sparkSession.implicits._

    variantRows
      .withColumn("sampleId", expr("genotypes[0].sampleId"))
      .select($"*", bin($"start", $"end", binSize))
      .withColumn("binStart", $"binId" * binSize)
      .withColumn("isDuplicate", $"start" < $"binStart")
      .repartition(numBinPartitions, $"contigName", $"binId")
      .sortWithinPartitions($"contigName", $"binId", $"start", $"sampleId", $"alternateAlleles")
  }

  /**
   * Generates re-genotyped variant calls from a DataFrame of variant rows, which must be
   * bin-partitioned and location-sorted.
   */
  private[joint] def generateJointCalls(
      header: VCFHeader,
      binnedVariantRows: DataFrame): DataFrame = {

    val inputSchema = binnedVariantRows.schema
    val outputSchema = VCFSchemaInferrer.inferSchema(true, true, header)
    val internalRowRDD = binnedVariantRows.queryExecution.toRdd.mapPartitions { iter =>
      val htsjdkVcConverter = new InternalRowToVariantContextConverter(
        inputSchema,
        header.getMetaDataInInputOrder.asScala.toSet,
        ValidationStringency.LENIENT
      )
      val internalRowConverter =
        new VariantContextToInternalRowConverter(header, outputSchema, ValidationStringency.LENIENT)

      val engine = JointlyCallVariants.setupGenotypeGvcfsEngine(header, getReferenceGenomeName)
      val merger = JointlyCallVariants.createMerger(header)
      val reference = ReferenceDataSource.of(Paths.get(getReferenceGenomeFastaPath))

      // Each partition is sorted by contigName, binId, and start position.
      // For each locus, we regenotype with the overlapping variant contexts.
      new RegenotypedVariantRowIterator(
        iter,
        inputSchema,
        header,
        htsjdkVcConverter,
        internalRowConverter,
        engine,
        merger,
        reference
      )
    }

    SQLUtils.internalCreateDataFrame(
      binnedVariantRows.sparkSession,
      internalRowRDD,
      outputSchema,
      isStreaming = false
    )
  }
}

object JointlyCallVariants {

  private def standardAnnotations: Seq[Annotation] = {
    val baseAnnotations = AssemblyShardCaller.getAnnotations(classOf[StandardAnnotation])
    baseAnnotations.foreach {
      // For backwards compatibility: allows us to annotate old RMSMappingQuality-annotated VCFs
      case ann: RMSMappingQuality => ann.allowOlderRawKeyValues = true
      case _ => // Do nothing
    }
    baseAnnotations
  }

  private def annotationEngine = new VariantAnnotatorEngine(
    standardAnnotations.asJava,
    dbsnp.dbsnp,
    /* featureInputs */ Collections.emptyList(),
    /* useRaw */ false,
    /* keepCombined */ false
  )

  private def dbsnp = new DbsnpArgumentCollection()

  private def vcfHeaderProcessingStep(referenceGenomeName: String): VCFHeaderLine = {
    val gatkVersion = VersionTracker.toolVersion("GATK")
    val params = Seq(
      s"jointGenotyper=GATK$gatkVersion/GenotypeGVCFs",
      s"refGenomeName=$referenceGenomeName"
    )
    new VCFHeaderLine("DatabricksParams", params.mkString(" "))
  }

  def createMerger(header: VCFHeader): ReferenceConfidenceVariantContextMerger = {
    new ReferenceConfidenceVariantContextMerger(
      annotationEngine,
      header,
      /* somaticInput */ false
    )
  }

  private def createGenotypeGVCFsEngine(header: VCFHeader): GenotypeGVCFsEngine = {
    val genotypeArgs = new GenotypeCalculationArgumentCollection()
    new GenotypeGVCFsEngine(
      annotationEngine,
      genotypeArgs,
      /* includeNonVariants */ false,
      header
    )
  }

  // Sets up the GenotypeGVCFsEngine's VCF writer and and returns the output header
  private def setupVCFWriter(
      engine: GenotypeGVCFsEngine,
      referenceGenomeName: String): VCFHeader = {

    val stream = new ByteArrayOutputStream()
    val vcfWriter = new VariantContextWriterBuilder()
      .clearOptions()
      .setOutputStream(stream)
      .build

    engine.setupVCFWriter(
      /* defaultToolVCFHeaderLines */ Set(vcfHeaderProcessingStep(referenceGenomeName)).asJava,
      /* keepCombined */ false,
      /* dbsnp */ dbsnp,
      /* vcfWriter */ vcfWriter
    )
    val headerString = stream.toString
    vcfWriter.close()

    VCFHeaderUtils.parseHeaderFromString(headerString)
  }

  def setupGenotypeGvcfsEngine(
      header: VCFHeader,
      referenceGenomeName: String): GenotypeGVCFsEngine = {
    val engine = createGenotypeGVCFsEngine(header)
    setupVCFWriter(engine, referenceGenomeName)
    engine
  }

  def getGenotypeGvcfsEngineHeader(header: VCFHeader, referenceGenomeName: String): VCFHeader = {
    val engine = createGenotypeGVCFsEngine(header)
    setupVCFWriter(engine, referenceGenomeName)
  }
}
