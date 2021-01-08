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

import scala.collection.JavaConverters._
import com.google.common.annotations.VisibleForTesting
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeader
import io.projectglow.common.logging.HlsUsageLogging
import io.projectglow.vcf.VCFHeaderUtils
import org.apache.commons.math3.util.CombinatoricsUtils
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.bdgenomics.adam.rdd.ADAMContext._
import io.projectglow.pipelines.dnaseq.{HasGvcfDeltaOutput, HasStringency, HasTargetedRegionsParams, HasVcfManifest, HasVcfValidation, Manifest}
import io.projectglow.pipelines.PipelineStage
import io.projectglow.pipelines.common.DeltaHelper
import io.projectglow.pipelines.dnaseq.{HasGvcfDeltaOutput, HasStringency, HasTargetedRegionsParams, HasVcfManifest, HasVcfValidation}
import io.projectglow.pipelines.sql.{HLSConf, VCFHeaderLoader}

class IngestVariants
    extends PipelineStage
    with HasGvcfDeltaOutput
    with HasTargetedRegionsParams
    with HasStringency
    with HasVcfManifest
    with HasVcfValidation {

  private val deltaStatsCollectionConfKey =
    "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols"
  var deltaStatsCollectionConfValue: String = _

  override def getOutputParams: Seq[Param[_]] = Seq(gvcfDeltaOutput)

  override def outputExists(session: SparkSession): Boolean = {
    if (exportGVCFToDelta) {
      DeltaHelper.tableExists(session, $(gvcfDeltaOutput), None)
    } else {
      // If Delta output path is empty, we don't want to output anyways
      true
    }
  }

  override def cleanup(session: SparkSession): Unit = {
    super.cleanup(session)

    // Return stats collection conf to original value
    if (SQLConf.get.getConf(HLSConf.VARIANT_INGEST_SKIP_STATS_COLLECTION)) {
      SQLConf.get.setConfString(deltaStatsCollectionConfKey, deltaStatsCollectionConfValue)
    }
  }

  override def init(): Unit = {
    super.init()

    // Stats collection can be unnecessarily expensive
    if (SQLConf.get.getConf(HLSConf.VARIANT_INGEST_SKIP_STATS_COLLECTION)) {
      deltaStatsCollectionConfValue = SQLConf.get.getConfString(deltaStatsCollectionConfKey, "32")
      SQLConf.get.setConfString(deltaStatsCollectionConfKey, "0")
    }
  }

  /**
   * Loads VCF files, splits them into single-sample rows, joins each with the targeted regions;
   * then writes to Delta.
   */
  override def execute(session: SparkSession): Unit = {
    if (exportGVCFToDelta) {
      val inputPathDs = Manifest.loadFilePaths($(vcfManifest))(session.sqlContext)

      val filteredVariantRowDf = IngestVariants.readAndFilter(
        inputPathDs,
        None,
        $(targetedRegions),
        $(performValidation),
        getValidationStringency
      )
      // If we don't coalesce, we get one partition per file part - this can make the driver OOM
      writeVcf(session, filteredVariantRowDf)
    } else {
      logger.warn(
        "No Delta output path for input GVCFs was provided, so the ingest step will be skipped."
      )
    }
  }

  private[joint] def writeVcf(session: SparkSession, df: DataFrame): Unit = {
    DeltaHelper.save(df, $(gvcfDeltaOutput), mode = $(saveMode))
  }
}

object IngestVariants extends HlsUsageLogging {

  /**
   * Creates a VCF header based on the manifest.
   */
  private[joint] def createVcfHeader(session: SparkSession, paths: Seq[String]): VCFHeader = {

    // We can't access Spark's internal file listing methods here, so the most straightforward
    // to get the set of all input paths is to create a DataFrame
    val files = session.read.text(paths: _*).inputFiles.toSeq

    val headerRDD = VCFHeaderUtils.createHeaderRDD(session, files)
    headerRDD.cache()
    try {
      val lines = VCFHeaderUtils.getUniqueHeaderLines(headerRDD, getNonSchemaHeaderLines = true)
      val sampleIds = VCFHeaderLoader.getAllSamples(headerRDD)
      new VCFHeader(lines.toSet.asJava, sampleIds.asJava)
    } finally {
      headerRDD.unpersist()
    }
  }

  /**
   * - Read single-sample variant rows from provided paths
   * - Filter rows by targeted regions (if provided)
   * - Filter rows by valid PL count (if provided)
   * - Coalesce rows to a reasonable number of partitions
   */
  private[joint] def readAndFilter(
      inputPathDs: Dataset[String],
      schemaOpt: Option[StructType],
      targetedRegionsPath: String,
      performValidation: Boolean,
      stringency: ValidationStringency): DataFrame = {
    val variantRows = readSingleSampleVariantRows(inputPathDs, schemaOpt, targetedRegionsPath)
    val filteredByValidCount = filterByValidPLCount(variantRows, performValidation, stringency)
    val numPartitions = SQLConf.get.getConf(HLSConf.VARIANT_INGEST_NUM_PARTITIONS)
    filteredByValidCount.coalesce(numPartitions)
  }

  /**
   * Filter a DataFrame based on a targeted regions file. Pushes down a datasource filter if the
   * number of targeted regions is sufficiently small.
   */
  private def filterByTargetedRegions(df: DataFrame, targetedRegionsPath: String): DataFrame = {
    val spark = df.sparkSession
    val features = spark.sparkContext.loadFeatures(targetedRegionsPath)
    val numFeatures = features.dataset.count()

    val maxNumFeatures = SQLConf.get.getConf(HLSConf.JOINT_GENOTYPING_MAX_PUSHDOWN_FILTERS)
    if (numFeatures <= maxNumFeatures) {
      import spark.implicits._
      val targetedRegionsCol = features
        .rdd
        .map { f =>
          col("contigName") === lit(f.referenceName) && col("start") < lit(f.end) && col("end") > lit(
            f.start
          )
        }
        .reduce(_ || _)
      df.filter(targetedRegionsCol)
    } else {
      val targetedRegionsDs = features.dataset
      // Uses range join optimization in DBR using a hint.
      val rangeJoinBinSize = SQLConf.get.getConf(HLSConf.JOINT_GENOTYPING_RANGE_JOIN_BIN_SIZE)
      // The set of targeted regions is relatively small, so we can perform a broadcast join.
      df.hint("range_join", rangeJoinBinSize)
        .join(
          broadcast(targetedRegionsDs),
          df("contigName") === targetedRegionsDs("referenceName") &&
          df("start") < targetedRegionsDs("end") &&
          df("end") > targetedRegionsDs("start"),
          "inner"
        )
        .select(df("*"))
        .withColumn("sampleId", expr("genotypes[0].sampleId"))
        .dropDuplicates(
          "contigName",
          "start",
          "end",
          "names",
          "referenceAllele",
          "alternateAlleles",
          "sampleId"
        )
    }
  }

  /**
   * Reads a DataFrame of variant rows from a path, splitting any multi-sample rows.
   */
  private def readSingleSampleVariantRows(
      inputPathDs: Dataset[String],
      schemaOpt: Option[StructType],
      targetedRegionsPath: String): DataFrame = {
    val paths = inputPathDs.collect
    val spark = inputPathDs.sparkSession
    val baseReader = spark.read.format("vcf")

    val reader = schemaOpt match {
      case Some(schema) => baseReader.schema(schema)
      case None => baseReader.option("includeSampleIds", true)
    }

    val baseDf = targetedRegionsPath match {
      case "" => reader.load(paths: _*)
      case t => filterByTargetedRegions(reader.load(paths: _*), t)
    }

    baseDf
      .withColumn("explodedGenotype", expr("explode(genotypes)"))
      .withColumn("genotypes", expr("array(explodedGenotype)"))
      .drop("explodedGenotype")
  }

  // Creates an expression that will get the size of a genotype subfield. If the subfield does not
  // exist, the size will default to 0.
  private def getSizeGenotypeFieldExpr(gtSchema: StructType, gtField: String): String = {
    if (gtSchema.fieldNames.contains(gtField)) {
      // Coalesce returns the first non-null argument
      s"coalesce(size(genotypes[0].$gtField), 0)"
    } else {
      "0"
    }
  }

  @VisibleForTesting
  private[joint] def filterByValidPLCount(
      df: DataFrame,
      performValidation: Boolean,
      validationStringency: ValidationStringency): DataFrame = {

    df.sparkSession
      .udf
      .register(
        "hasValidPLs",
        hasValidPLCount(_: Int, _: Int, _: Int, _: Int, validationStringency)
      )

    if (!performValidation) {
      return df
    }

    val gtSchema = df
      .schema
      .fields
      .find(_.name == "genotypes")
      .get
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]

    val numCallsExpr = getSizeGenotypeFieldExpr(gtSchema, "calls")
    val numPlsExpr = getSizeGenotypeFieldExpr(gtSchema, "phredLikelihoods")
    val numGlsExpr = getSizeGenotypeFieldExpr(gtSchema, "genotypeLikelihoods")

    df.where(
      callUDF(
        "hasValidPLs",
        expr("size(alternateAlleles)"),
        expr(numCallsExpr),
        expr(numPlsExpr),
        expr(numGlsExpr)
      )
    )
  }

  /**
   * Validates that a VCF row has either no PLs, or the correct number of PLs for the number of
   * alternate alleles.
   *
   * Some germline variant calling pipelines generate incorrect gVCF data.
   * Specifically, we have observed that Edico generates gVCF lines with
   * incorrect PL counts for sites with a very large number of candidate
   * alternate alleles (e.g., >20 alts).
   *
   * @param validationStringency If STRICT, then throw an exception on an invalid row.
   *                             If LENIENT, then print an error message on an invalid row,
   *                             and return false. If SILENT, then return false.
   * @return If the row passes validation, returns true. Otherwise, if the validation stringency is
   *         SILENT or LENIENT, returns false (throws error on STRICT).
   */
  @VisibleForTesting
  private[joint] def hasValidPLCount(
      numAltAlleles: Int,
      copyNumber: Int,
      numPls: Int,
      numGls: Int,
      validationStringency: ValidationStringency): Boolean = {

    if (copyNumber < 1) {
      // No calls, so we can't check for copy number
      return true
    }

    // Assumes there is a reference allele
    val numAlleles = 1 + numAltAlleles

    // Multisubset of size (copyNumber) from set of size (numAlleles)
    val correctPlLength =
      CombinatoricsUtils.binomialCoefficient(copyNumber + numAlleles - 1, copyNumber).toInt
    val actualPlLengthOpt = if (numPls > 0) {
      Some(numPls)
    } else if (numGls > 0) {
      Some(numGls)
    } else {
      None
    }

    // The GATK breaks if there are greater than 0 PL fields, but fewer than the correct number
    val isValid = actualPlLengthOpt.isEmpty || correctPlLength <= actualPlLengthOpt.get
    if (isValid) {
      true
    } else {
      // Fewer PLs than expected
      lazy val warning = s"Found VCF record with invalid PL length (should be $correctPlLength, " +
        s"actually is ${actualPlLengthOpt.get})"
      if (validationStringency == ValidationStringency.STRICT) {
        throw new IllegalArgumentException(warning)
      } else if (validationStringency == ValidationStringency.LENIENT) {
        logger.warn(warning)
      }
      false
    }
  }

}
