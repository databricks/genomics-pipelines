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

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Files

import scala.collection.JavaConverters._
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{Allele, GenotypeBuilder, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFCompoundHeaderLine, VCFHeader}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLUtils}
import io.projectglow.vcf.{VCFMetadataLoader, VCFSchemaInferrer, VariantContextToInternalRowConverter}
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.common.DeltaHelper
import io.projectglow.pipelines.dnaseq.Manifest
import io.projectglow.pipelines.sql.HLSConf

class IngestVariantsSuite extends PipelineBaseTest {

  val vcfPath = s"$testDataHome/joint/HG00096.chr20_18034651_18034655.g.vcf"
  lazy val header = VCFMetadataLoader
    .readVcfHeader(
      spark.sparkContext.hadoopConfiguration,
      vcfPath
    )

  lazy val headerLines = header.getMetaDataInInputOrder.asScala.toSet
  lazy val schema = VCFSchemaInferrer.inferSchema(true, true, header)
  lazy val rowConverter = new VariantContextToInternalRowConverter(
    header,
    schema,
    ValidationStringency.LENIENT,
    writeSampleIds = true
  )
  lazy val refAllele = Allele.REF_A
  lazy val altAllele = Allele.ALT_C

  private def checkNoCachedRDDs(): Unit = {
    eventually {
      assert(sparkContext.getPersistentRDDs.isEmpty)
    }
  }

  def createDeltaOutput: String = Files.createTempDirectory("delta-data").toString

  def createVariantIngester(locus: String, deltaOutput: String): IngestVariants = {
    new IngestVariants()
      .setVcfManifest(s"$testDataHome/joint/manifest.$locus.csv".toString)
      .setGvcfDeltaOutput(deltaOutput)
  }

  def checkTargetedRegions(targetedRegions: String, numRows: Int): Unit = {
    val variantIngester = createVariantIngester("chr20_18034651_18034655", createDeltaOutput)
      .setTargetedRegions(targetedRegions)
    variantIngester.init()
    variantIngester.execute(spark)

    val df = spark.read.format("delta").load(variantIngester.getGvcfDeltaOutput)
    assert(df.count == numRows)
  }

  def checkPushdownFilter(targetedRegions: String, shouldPushdown: Boolean): Unit = {
    val manifest = s"$testDataHome/joint/manifest.chr20_18034651_18034655.csv"
    val inputPathDs = Manifest.loadFilePaths(manifest)(spark.sqlContext)
    val filteredDs = IngestVariants.readAndFilter(
      inputPathDs,
      None,
      targetedRegions,
      false,
      ValidationStringency.LENIENT
    )

    val physicalPlan = filteredDs.queryExecution.simpleString
    val didPushdown =
      physicalPlan.contains("PushedFilters: [IsNotNull(contigName), EqualTo(contigName,chr20)")
    assert(didPushdown == shouldPushdown)
  }

  test("Multiple sites") {
    val variantIngester = createVariantIngester("chr20_18034651_18034655", createDeltaOutput)
    variantIngester.init()
    variantIngester.execute(spark)

    val df = spark.read.format("delta").load(variantIngester.getGvcfDeltaOutput)
    assert(df.schema == schema)
    assert(df.count == 14)
  }

  test("Multiple sites with interval") {
    checkTargetedRegions(s"$testDataHome/joint/chr20_18034651.bed", 4)
  }

  test("Multiple sites with small number of intervals pushed down") {
    val targetedRegions = s"$testDataHome/joint/chr20_18034651_18034655.bed"
    checkTargetedRegions(targetedRegions, 9)
    checkPushdownFilter(targetedRegions, true)
  }

  test("Multiple sites with large number of intervals not pushed down") {
    val targetedRegions = s"$testDataHome/joint/chr20_18034500_18034800.bed"
    checkTargetedRegions(targetedRegions, 10)
    checkPushdownFilter(targetedRegions, false)
  }

  test("Multiple sites with small number of intervals not pushed down") {
    val defaultMaxPushdownFilters =
      SQLConf.get.getConf(HLSConf.JOINT_GENOTYPING_MAX_PUSHDOWN_FILTERS)
    SQLConf.get.setConf(HLSConf.JOINT_GENOTYPING_MAX_PUSHDOWN_FILTERS, 1)

    val targetedRegions = s"$testDataHome/joint/chr20_18034651_18034655.bed"
    checkTargetedRegions(targetedRegions, 9)
    checkPushdownFilter(targetedRegions, false)

    SQLConf.get.setConf(HLSConf.JOINT_GENOTYPING_MAX_PUSHDOWN_FILTERS, defaultMaxPushdownFilters)
  }

  Seq(1, 2, 3).foreach { batchSize =>
    test(s"Save across batches of size $batchSize") {
      SQLConf.get.setConf(HLSConf.VARIANT_INGEST_BATCH_SIZE, batchSize)

      val deltaOutput = createDeltaOutput

      val variantIngester = createVariantIngester("chr20_18034651_18034655", deltaOutput)
      variantIngester.init()
      variantIngester.execute(spark)

      val df = spark.read.format("delta").load(deltaOutput)
      assert(df.count == 14)
    }
  }

  Seq(1, 2, 3).foreach { batchSize =>
    test(s"Save mode: append, batch size $batchSize") {
      SQLConf.get.setConf(HLSConf.VARIANT_INGEST_BATCH_SIZE, batchSize)

      val deltaOutput = createDeltaOutput

      val variantIngesterOne = createVariantIngester("chr20_17960111", deltaOutput)
      variantIngesterOne.init()
      variantIngesterOne.execute(spark)

      val df1 = spark.read.format("delta").load(deltaOutput)
      assert(df1.count == 3)

      val variantIngesterTwo = createVariantIngester("chr20_18034651_18034655", deltaOutput)
        .setSaveMode("append")
      variantIngesterTwo.init()
      variantIngesterTwo.execute(spark)

      val df2 = spark.read.format("delta").load(deltaOutput)
      assert(df2.count == 17)
    }
  }

  Seq(1, 2, 3).foreach { batchSize =>
    test(s"Save mode: overwrite, batch size $batchSize") {
      SQLConf.get.setConf(HLSConf.VARIANT_INGEST_BATCH_SIZE, batchSize)

      val deltaOutput = createDeltaOutput

      val variantIngesterOne = createVariantIngester("chr20_17960111", deltaOutput)
      variantIngesterOne.init()
      variantIngesterOne.execute(spark)

      val df1 = spark.read.format("delta").load(deltaOutput)
      assert(df1.count == 3)

      val variantIngesterTwo = createVariantIngester("chr20_18034651_18034655", deltaOutput)
        .setSaveMode("overwrite")
      variantIngesterTwo.init()
      variantIngesterTwo.execute(spark)

      val df2 = spark.read.format("delta").load(deltaOutput)
      assert(df2.count == 14)
    }
  }

  Seq(1, 2, 3).foreach { batchSize =>
    test(s"Save mode: errorIfExists, batch size $batchSize") {
      SQLConf.get.setConf(HLSConf.VARIANT_INGEST_BATCH_SIZE, batchSize)

      val deltaOutput = createDeltaOutput

      val variantIngesterOne = createVariantIngester("chr20_17960111", deltaOutput)
      variantIngesterOne.init()
      variantIngesterOne.execute(spark)

      val df1 = spark.read.format("delta").load(deltaOutput)
      assert(df1.count == 3)

      val variantIngesterTwo = createVariantIngester("chr20_18034651_18034655", deltaOutput)
        .setSaveMode("errorIfExists")
      variantIngesterTwo.init()
      assertThrows[AnalysisException](variantIngesterTwo.execute(spark))

      val df2 = spark.read.format("delta").load(deltaOutput)
      assert(df2.count == 3)
    }
  }

  Seq(1, 2, 3).foreach { batchSize =>
    test(s"Save mode: ignore, batch size $batchSize") {
      SQLConf.get.setConf(HLSConf.VARIANT_INGEST_BATCH_SIZE, batchSize)

      val deltaOutput = createDeltaOutput

      val variantIngesterOne = createVariantIngester("chr20_17960111", deltaOutput)
      variantIngesterOne.init()
      variantIngesterOne.execute(spark)

      val df1 = spark.read.format("delta").load(deltaOutput)
      assert(df1.count == 3)

      val variantIngesterTwo = createVariantIngester("chr20_18034651_18034655", deltaOutput)
        .setSaveMode("ignore")
      variantIngesterTwo.init()
      variantIngesterTwo.execute(spark)

      val df2 = spark.read.format("delta").load(deltaOutput)
      assert(df2.count == 3)
    }
  }

  test("Invalid save mode") {
    val variantIngester = createVariantIngester("chr20_18034651_18034655", createDeltaOutput)
      .setSaveMode("invalidSaveMode")
    variantIngester.init()
    assertThrows[IllegalArgumentException](variantIngester.execute(spark))
  }

  test("Splits input GVCF") {
    val tempManifest = Files.createTempFile("manifest", ".csv").toString

    val bw = new BufferedWriter(new FileWriter(tempManifest))
    bw.write(s"$testDataHome/combined.chr20_18210071_18210093.g.vcf")
    bw.close()

    val variantIngester = new IngestVariants()
      .setVcfManifest(tempManifest)
      .setGvcfDeltaOutput(createDeltaOutput)
    variantIngester.init()
    variantIngester.execute(spark)

    val df = spark.read.format("delta").load(variantIngester.getGvcfDeltaOutput)
    assert(df.count == 69)
    assert(df.dropDuplicates("contigName", "start", "end").count == 23)
  }

  test("replay mode") {
    val dir = Files.createTempDirectory("replay")
    val stage = new IngestVariants()
      .setGvcfDeltaOutput(dir.toString)
    stage.init()
    assert(!stage.outputExists(spark))
    DeltaHelper.save(spark.range(1), stage.getGvcfDeltaOutput)
    assert(stage.outputExists(spark))
  }

  test("validation will return true on a row with proper PLs") {
    val alts = 1
    val cn = 2
    val pls = 3
    val gls = 0

    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.STRICT))
    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.LENIENT))
    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.SILENT))
  }

  test("validation will return true on a row with no PLs") {
    val alts = 1
    val cn = 2
    val pls = 0
    val gls = 0
    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.STRICT))
    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.LENIENT))
    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.SILENT))
  }

  test("validation will return true on a row with too many PLs") {
    val alts = 1
    val cn = 2
    val pls = 4
    val gls = 0

    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.STRICT))
    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.LENIENT))
    assert(IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.SILENT))
  }

  test("validation will return false on a row with fewer PLs than expected") {
    val alts = 1
    val cn = 2
    val pls = 2
    val gls = 0

    assertThrows[IllegalArgumentException] {
      IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.STRICT)
    }
    assert(!IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.LENIENT))
    assert(!IngestVariants.hasValidPLCount(alts, cn, pls, gls, ValidationStringency.SILENT))
  }

  def makeDataFrame(gts: Seq[InternalRow]): DataFrame = {
    SQLUtils.internalCreateDataFrame(
      spark,
      spark.sparkContext.parallelize(gts),
      schema,
      isStreaming = false
    )
  }

  def numFilteredTrio(performValidation: Boolean, stringency: ValidationStringency): Long = {
    IngestVariants
      .filterByValidPLCount(
        makeGtTrio,
        performValidation,
        stringency
      )
      .count
  }

  test("skipping validation will not drop any rows") {
    assert(numFilteredTrio(false, ValidationStringency.SILENT) == 3)
  }

  test("strict validation will fire an exception") {
    assertThrows[SparkException] {
      numFilteredTrio(true, ValidationStringency.STRICT)
    }
  }

  test("lenient validation will drop malformed records") {
    assert(numFilteredTrio(true, ValidationStringency.LENIENT) == 2)
  }

  test("silent validation will drop malformed records") {
    assert(numFilteredTrio(true, ValidationStringency.SILENT) == 2)
  }

  test("don't drop records if copy number can't be determined") {
    val gts = Seq(makeGt(Seq(1, 2, 3), Seq.empty), makeGt(Seq(1, 2, 3), Seq.empty))
    val count = IngestVariants
      .filterByValidPLCount(
        makeDataFrame(gts),
        performValidation = true,
        ValidationStringency.LENIENT
      )
      .count()
    assert(count == 2)
  }

  test("don't drop records without PLs") {
    val gts = Seq(makeGt(Seq.empty))
    val count = IngestVariants
      .filterByValidPLCount(
        makeDataFrame(gts),
        performValidation = true,
        ValidationStringency.LENIENT
      )
      .count()
    assert(count == 1)
  }

  def makeGtTrio: DataFrame = {
    makeDataFrame(
      Seq(
        makeGt(Seq.empty),
        makeGt(Seq(0, 1)),
        makeGt(Seq(0, 1, 0))
      )
    )
  }

  def makeGt(pls: Seq[Int], alleles: Seq[Allele] = Seq(refAllele, altAllele)): InternalRow = {
    val vcb = new VariantContextBuilder(
      "Unknown",
      "1",
      1001L,
      1001L,
      Seq(Allele.REF_A, Allele.ALT_C).asJava
    )
    val gt = new GenotypeBuilder("mySample", alleles.asJava)
      .phased(true)
      .PL(pls.toArray)
      .make
    val vc = vcb.genotypes(gt).make
    rowConverter.convertRow(vc, false)
  }

  test("Do nothing if do not export GVCF to Delta") {
    val variantIngester = createVariantIngester("chr20_18034651_18034655", "")
    variantIngester.init()
    variantIngester.execute(spark)

    // Shouldn't crash
  }

  test("Skip stats collection by default") {
    val variantIngester = createVariantIngester("chr20_17960111", createDeltaOutput)

    variantIngester.init()
    assert(
      SQLConf
        .get
        .getConfString(
          "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols"
        ) == "0"
    )
  }

  Seq(true, false).foreach { skipStatsCollConf =>
    test(s"Skip stats collection based on conf: $skipStatsCollConf") {
      SQLConf.get.setConf(HLSConf.VARIANT_INGEST_SKIP_STATS_COLLECTION, skipStatsCollConf)
      SQLConf
        .get
        .setConfString(
          "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols",
          "32"
        )

      val variantIngester = createVariantIngester("chr20_17960111", createDeltaOutput)

      variantIngester.init()
      val skipStatsColl = SQLConf
          .get
          .getConfString(
            "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols"
          ) == "0"
      assert(skipStatsColl == skipStatsCollConf)
    }
  }

  test("Cleanup stats collection conf") {
    SQLConf.get.setConf(HLSConf.VARIANT_INGEST_SKIP_STATS_COLLECTION, true)

    val variantIngester = createVariantIngester("chr20_17960111", createDeltaOutput)

    SQLConf
      .get
      .setConfString("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "32")

    variantIngester.init()
    variantIngester.cleanup(spark)
    assert(
      SQLConf
        .get
        .getConfString(
          "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols"
        ) == "32"
    )
  }

  test("Load directory") {
    val variantIngester = createVariantIngester(
      "chr21_575_split_directory",
      createDeltaOutput
    )
    variantIngester.init()
    variantIngester.execute(spark)
    // Shouldn't break
  }

  test("Load glob") {
    val variantIngester = createVariantIngester("chr21_575_split_glob", createDeltaOutput)
    variantIngester.init()
    variantIngester.execute(spark)
    // Shouldn't break
  }

  test("Load glob with directory") {
    val variantIngester = createVariantIngester("chr21_575_split_glob_dir", createDeltaOutput)
    variantIngester.init()
    variantIngester.execute(spark)
    // Shouldn't break
  }

  test("Throws error if strict validation") {
    val variantIngester = createVariantIngester("malformed", createDeltaOutput)
      .setPerformValidation(true)
      .setValidationStringency(ValidationStringency.STRICT)
    variantIngester.init()
    val caught = intercept[SparkException] {
      variantIngester.execute(spark)
    }
    assert(caught.getCause.getCause.getCause.isInstanceOf[SparkException])
  }

  Seq(ValidationStringency.LENIENT, ValidationStringency.SILENT).foreach { vs =>
    test(s"Apply filter quietly if $vs") {
      val variantIngester = createVariantIngester("malformed", createDeltaOutput)
        .setPerformValidation(true)
        .setValidationStringency(vs)
      variantIngester.init()
      variantIngester.execute(spark)
      variantIngester.cleanup(spark)

      val ds = spark.read.format("delta").load(variantIngester.getGvcfDeltaOutput)
      assert(ds.count == 4)
    }
  }

  test("Do not perform validation, validation stringency strict by default") {
    val variantIngester = new IngestVariants()
    assert(!variantIngester.getPerformValidation)
    assert(variantIngester.getValidationStringency == ValidationStringency.STRICT)
  }

  private def getSortedLines(header: VCFHeader): Seq[VCFCompoundHeaderLine] = {
    (header.getFormatHeaderLines.asScala ++ header.getInfoHeaderLines.asScala)
      .toSeq
      .sortBy(l => (l.getClass.getSimpleName, l.getID))
  }

  test("read header (1 file)") {
    val testHeader = IngestVariants.createVcfHeader(spark, Seq(vcfPath))
    val lines =
      (testHeader.getInfoHeaderLines.asScala ++ testHeader.getFormatHeaderLines.asScala)
        .toSeq
        .sortBy(_.getID)
    val goldenLines =
      (header.getInfoHeaderLines.asScala ++ header.getFormatHeaderLines.asScala)
        .toSeq
        .sortBy(_.getID)
    assert(lines == goldenLines)
    assert(testHeader.getGenotypeSamples.asScala == header.getGenotypeSamples.asScala)
  }

  test("read header (multiple files)") {
    val paths = Seq(
      s"$testDataHome/dnaseq/NA12878_21_10002403.g.vcf",
      s"$testDataHome/1kg_sample.vcf"
    )
    val jointlyReadHeader = IngestVariants.createVcfHeader(spark, paths)
    val headers =
      paths.map(VCFMetadataLoader.readVcfHeader(spark.sparkContext.hadoopConfiguration, _))
    val lines = headers
      .map(getSortedLines)
      .reduce(_ ++ _)
      .map(h => (h.getClass.getSimpleName, h.getID))
      .distinct
      .sorted
    val ourLines = getSortedLines(jointlyReadHeader).map(l => (l.getClass.getSimpleName, l.getID))
    assert(lines == ourLines)
    assert(
      headers
        .map(_.getGenotypeSamples.asScala)
        .reduce(_ ++ _)
        .sorted == jointlyReadHeader.getGenotypeSamples.asScala
    )
  }

  test("uncache RDD after reading header") {
    IngestVariants.createVcfHeader(spark, Seq(vcfPath))
    checkNoCachedRDDs()
  }

  test("uncache RDD if header cannot be read") {
    val path = Files.createTempDirectory("badvcf").resolve("file.vcf")
    FileUtils.writeStringToFile(path.toFile, "monkey")
    intercept[SparkException](IngestVariants.createVcfHeader(spark, Seq(path.toString)))
    checkNoCachedRDDs()
  }

  test("Must provide manifest") {
    val variantIngester = new IngestVariants().setGvcfDeltaOutput(createDeltaOutput)
    variantIngester.init()
    assertThrows[IllegalArgumentException](variantIngester.execute(spark))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    SQLConf
      .get
      .setConfString(
        HLSConf.JOINT_GENOTYPING_NUM_SHUFFLE_PARTITIONS.toString,
        spark.sessionState.conf.numShufflePartitions.toString
      )
  }
}
