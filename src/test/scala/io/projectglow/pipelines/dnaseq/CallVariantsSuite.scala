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

import java.nio.file.Files

import htsjdk.samtools.ValidationStringency
import io.projectglow.vcf.VCFMetadataLoader
import org.apache.spark.sql.functions
import io.projectglow.pipelines.{PipelineBaseTest, SimplePipeline}
import io.projectglow.pipelines.{PipelineBaseTest, SimplePipeline}
import io.projectglow.pipelines.common.{DeltaHelper, VersionTracker}

class CallVariantsSuite extends PipelineBaseTest {

  def createVariantCaller: CallVariants = {
    val outputDir = Files.createTempDirectory("output")
    new CallVariants()
      .setReferenceGenomeFastaPath(referenceGenomeFastaPath)
      .setOutput(outputDir.toString)
  }

  test("execute") {
    val alignmentOutput = s"$testDataHome/raw.reads/GIAB.NA12878.20p12.1/NA12878.20p12.1.30x.bam"

    val callVariants = createVariantCaller
      .setSampleMetadata(SampleMetadata("NA12878", Seq(ReadGroupMetadata("", ""))))
      .setAlignmentOutput(alignmentOutput)
      .setExportVCF(true)

    val pipeline = new SimplePipeline("test", Seq(callVariants))
    pipeline.init(Map())
    pipeline.execute(spark)

    assert(callVariants.outputExists(spark))

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val variantRows = spark.read.format("delta").load(callVariants.getCalledVariantOutput)
    val allelicBalance = variantRows
      .selectExpr("genotypes[0] as gt")
      .filter(functions.size(functions.array_distinct($"gt.calls")) >= 2) // heterozygotes
      .filter($"gt.depth" > 6)
      .withColumn("refReadDepth", functions.expr("gt.alleleDepths[0]"))
      .withColumn("altReadDepth", functions.expr("gt.alleleDepths[1]"))
      .withColumn("allelicBalance", $"refReadDepth" / ($"refReadDepth" + $"altReadDepth"))

    val count = allelicBalance.count()
    val normal = allelicBalance
      .filter(!($"allelicBalance" > 0.7 || $"allelicBalance" < 0.3))
      .count()

    assert(count ~== normal relTol 0.2)

    // check that the VCF was exported correctly
    val vcf = spark
      .read
      .format("vcf")
      .option("includeSampleIds", "true")
      .load(callVariants.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration))
    assert(
      vcf.filter($"genotypes.sampleId" === functions.typedLit(Seq("NA12878"))).count == vcf.count
    )
  }

  test("call variants on NA12878") {
    val alignmentOutput = s"$bigFilesHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam"

    val callVariants = createVariantCaller
      .setSampleMetadata(SampleMetadata("NA12878", Seq(ReadGroupMetadata("", ""))))
      .setMaxReadsPerPosition(0)
      .setAlignmentOutput(alignmentOutput)
      .setValidationStringency(ValidationStringency.LENIENT)
      .setExportVCF(true)
    val pipeline = new SimplePipeline("test", Seq(callVariants))
    pipeline.init(Map())
    pipeline.execute(spark)

    compareVcfs(
      s"$testDataHome/dnaseq/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf",
      callVariants.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration)
    )
  }

  test("call variants on small NA12878") {
    val alignmentOutput = s"$testDataHome/NA12878_21_10002403.bam"

    val callVariants = createVariantCaller
      .setSampleMetadata(SampleMetadata("NA12878", Seq(ReadGroupMetadata("", ""))))
      .setAlignmentOutput(alignmentOutput)
      .setValidationStringency(ValidationStringency.LENIENT)
      .setExportVCF(true)
    val pipeline = new SimplePipeline("test", Seq(callVariants))
    pipeline.init(Map())
    pipeline.execute(spark)

    compareVcfs(
      s"$testDataHome/dnaseq/NA12878_21_10002403.vcf",
      callVariants.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration)
    )
  }

  test("call variants without alignment output") {
    val alignedReads = s"$testDataHome/NA12878_21_10002403.bam"

    val callVariants = createVariantCaller
      .setSampleMetadata(SampleMetadata("NA12878", Seq(ReadGroupMetadata("NA12878", alignedReads))))
      .setValidationStringency(ValidationStringency.LENIENT)
      .setExportVCF(true)
    val pipeline = new SimplePipeline("test", Seq(callVariants))
    pipeline.init(Map())
    pipeline.execute(spark)

    compareVcfs(
      s"$testDataHome/dnaseq/NA12878_21_10002403.vcf",
      callVariants.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration)
    )
  }

  test("VCF formatting") {
    val alignmentOutput = s"$testDataHome/NA12878_21_10002403.bam"

    val callVariants = createVariantCaller
      .setSampleMetadata(SampleMetadata("NA12878", Seq(ReadGroupMetadata("", ""))))
      .setAlignmentOutput(alignmentOutput)
      .setValidationStringency(ValidationStringency.LENIENT)
      .setExportVCF(true)
    val pipeline = new SimplePipeline("test", Seq(callVariants))
    pipeline.init(Map())
    pipeline.execute(spark)

    // includes databricks params vcf header
    val vcfHeader = VCFMetadataLoader.readVcfHeader(
      spark.sparkContext.hadoopConfiguration,
      callVariants.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration)
    )
    assert(
      vcfHeader
        .getMetaDataLine("DatabricksParams")
        .getValue
        .contains(s"variantCaller=GATK${VersionTracker.toolVersion("GATK")}/HaplotypeCaller")
    )
  }

  test("call variants on small NA12878 and produce a BP resolution gVCF") {
    val alignmentOutput = s"$testDataHome/NA12878_21_10002403.bam"
    val bedPath = s"$testDataHome/21_10002393_10002413.bed"

    val callVariants = createVariantCaller
      .setSampleMetadata(SampleMetadata("NA12878", Seq(ReadGroupMetadata("", ""))))
      .setAlignmentOutput(alignmentOutput)
      .setValidationStringency(ValidationStringency.LENIENT)
      .setReferenceConfidenceMode("BP_RESOLUTION")
      .setTargetedRegions(bedPath)
      .setExportVCF(true)
    val pipeline = new SimplePipeline("test", Seq(callVariants))
    pipeline.init(Map())

    pipeline.execute(spark)

    compareVcfs(
      s"$testDataHome/dnaseq/NA12878_21_10002403.bp.g.vcf",
      callVariants.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration)
    )
  }

  test("call variants on small NA12878 and produce a banded gVCF") {
    val alignmentOutput = s"$testDataHome/NA12878_21_10002403.bam"
    val bedPath = s"$testDataHome/21_10002393_10002413.bed"

    val callVariants = createVariantCaller
      .setSampleMetadata(SampleMetadata("NA12878", Seq(ReadGroupMetadata("", ""))))
      .setAlignmentOutput(alignmentOutput)
      .setValidationStringency(ValidationStringency.LENIENT)
      .setReferenceConfidenceMode("GVCF")
      .setTargetedRegions(bedPath)
      .setExportVCF(true)
    val pipeline = new SimplePipeline("test", Seq(callVariants))
    pipeline.init(Map())
    pipeline.execute(spark)

    compareVcfs(
      s"$testDataHome/dnaseq/NA12878_21_10002403.g.vcf",
      callVariants.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration)
    )
  }

  test("replay mode") {
    val dir = Files.createTempDirectory("replay")
    val stage = new CallVariants()
      .setSampleMetadata(SampleMetadata("sample", Seq.empty))
      .setOutput(dir.toString)
    stage.init()
    assert(!stage.outputExists(spark))
    val df = spark.range(1).withColumn("sampleId", functions.lit("sample"))
    DeltaHelper.save(
      df,
      dir.resolve("genotypes").toString,
      Option(("sampleId", Set("sample")))
    )
    assert(stage.outputExists(spark))
  }
}
