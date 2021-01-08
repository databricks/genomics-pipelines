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

import java.nio.file.Files

import io.projectglow.vcf.VCFMetadataLoader
import org.apache.spark.sql.{functions, DataFrame}
import io.projectglow.pipelines.dnaseq.{ReadGroupMetadata, SampleMetadata}
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.common.DeltaHelper
import io.projectglow.pipelines.dnaseq.{ReadGroupMetadata, SampleMetadata}

class CallMutationsSuite extends PipelineBaseTest {
  case class TestData(
      normalPath: String,
      tumorPath: String,
      exclusionsPath: String,
      normalSampleName: String,
      tumorSampleName: String,
      goldenVcfPath: String)

  val testData1 = TestData(
    s"$bigFilesHome/mutseq/normal_1.bam",
    s"$bigFilesHome/mutseq/tumor_1.bam",
    s"$bigFilesHome/mutseq/mask1.bed",
    "synthetic.challenge.set1.normal",
    "tumor sample",
    s"$bigFilesHome/mutseq/golden_1.vcf"
  )

  val testCases = Seq(
    testData1,
    TestData(
      s"$bigFilesHome/mutseq/normal_2.bam",
      s"$bigFilesHome/mutseq/tumor_2.bam",
      s"$bigFilesHome/mutseq/mask2.bed",
      "synthetic.challenge.set2.normal",
      "background.synth.challenge2.snvs.svs.tumorbackground",
      s"$bigFilesHome/mutseq/golden_2.vcf"
    ),
    TestData(
      s"$bigFilesHome/mutseq/normal_3.bam",
      s"$bigFilesHome/mutseq/tumor_3.bam",
      s"$bigFilesHome/mutseq/mask3.bed",
      "G15512.prenormal.sorted",
      "IS3.snv.indel.sv",
      s"$bigFilesHome/mutseq/golden_3.vcf"
    ),
    TestData(
      s"$bigFilesHome/mutseq/normal_4.bam",
      s"$bigFilesHome/mutseq/tumor_4.bam",
      s"$bigFilesHome/mutseq/mask4.bed",
      "synthetic.challenge.set4.normal",
      "synthetic.challenge.set4.tumour",
      s"$bigFilesHome/mutseq/golden_4.vcf"
    )
  )

  private def prepTumor(df: DataFrame, tumorSampleName: String): DataFrame = {
    df.withColumn("explodedGenotype", functions.expr("explode(genotypes)"))
      .withColumn("genotypes", functions.expr("array(explodedGenotype)"))
      .drop("explodedGenotype")
      .filter(s"genotypes[0].sampleId = '$tumorSampleName'")
      .orderBy("contigName", "start")
  }

  gridTest("matches gatk")(testCases) { testData =>
    val sampleMetadata = GroupedSampleMetadata(
      "pair",
      Seq(
        ("tumor", SampleMetadata(testData.tumorSampleName, Seq(ReadGroupMetadata("tumor", "")))),
        (
          "normal",
          SampleMetadata(testData.normalSampleName, Seq(ReadGroupMetadata("normal", "")))
        )
      )
    )
    val outputDir = Files.createTempDirectory("callmutations").toAbsolutePath.toString
    val stage = new CallMutations()
      .setGroupedSampleMetadata(sampleMetadata)
      .setNormalAlignmentOutput(testData.normalPath)
      .setTumorAlignmentOutput(testData.tumorPath)
      .setExcludedRegions(testData.exclusionsPath)
      .setReferenceGenomeFastaPath(referenceGenomeFastaPath)
      .setExportVCF(true)
      .setOutput(outputDir)

    stage.init()
    stage.execute(spark)
    assert(stage.outputExists(spark))

    val outputVc = spark
      .read
      .format("vcf")
      .option("includeSampleIds", "true")
      .load(stage.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration))
    val output = prepTumor(outputVc, testData.tumorSampleName)

    val goldenVc = spark
      .read
      .format("vcf")
      .option("includeSampleIds", "true")
      .load(testData.goldenVcfPath)
    val golden = prepTumor(goldenVc, testData.tumorSampleName)

    compareVariantRows(golden, output)

    // includes databricks params vcf header
    val vcfHeader = VCFMetadataLoader.readVcfHeader(
      spark.sparkContext.hadoopConfiguration,
      stage.getCalledVariantOutputVCF(spark.sparkContext.hadoopConfiguration)
    )
    assert(
      vcfHeader
        .getMetaDataLine("DatabricksParams")
        .getValue
        .contains("variantCaller=GATK4.1.4.1/Mutect2")
    )
  }

  test("panel of normals") {
    val testData = testData1
    val sampleMetadata = GroupedSampleMetadata(
      "pair",
      Seq(
        ("tumor", SampleMetadata(testData.tumorSampleName, Seq(ReadGroupMetadata("tumor", "")))),
        ("normal", SampleMetadata(testData.normalSampleName, Seq(ReadGroupMetadata("normal", ""))))
      )
    )
    val outputDir = Files.createTempDirectory("callmutations").toAbsolutePath.toString
    val stage = new CallMutations()
      .setGroupedSampleMetadata(sampleMetadata)
      .setNormalAlignmentOutput(testData.normalPath)
      .setTumorAlignmentOutput(testData.tumorPath)
      .setTargetedRegions(s"$bigFilesHome/mutseq/pon_1.bed")
      .setReferenceGenomeFastaPath(referenceGenomeFastaPath)
      .setExportVCF(true)
      .setOutput(outputDir)

    stage.init()
    stage.execute(spark)
    assert(!spark.read.format("delta").load(stage.getCalledVariantOutput).isEmpty)

    // This PoN VCF contains the only site that was emitted before
    stage.setPanelOfNormals(s"$bigFilesHome/mutseq/pon_1.vcf")
    stage.init()
    stage.execute(spark)
    assert(spark.read.format("delta").load(stage.getCalledMutationRoot).isEmpty)
  }

  test("replay mode") {
    val sampleMetadata = GroupedSampleMetadata("pair", Seq.empty)
    val dir = Files.createTempDirectory("replay")
    val stage = new CallMutations()
      .setOutput(dir.toString)
      .setGroupedSampleMetadata(sampleMetadata)
    stage.init()
    assert(!stage.outputExists(spark))
    val df = spark.range(1).withColumn("sampleId", functions.lit("pair"))
    DeltaHelper.save(df, stage.getCalledMutationRoot, Option(("sampleId", Set("pair"))))
    assert(stage.outputExists(spark))
  }
}
