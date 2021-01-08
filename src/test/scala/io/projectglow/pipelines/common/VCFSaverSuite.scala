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

package io.projectglow.pipelines.common

import scala.collection.JavaConverters._
import java.io.File
import java.nio.file.{Files, Paths}

import htsjdk.variant.variantcontext.writer.VCFHeaderWriter
import htsjdk.variant.vcf.VCFHeader
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.vcf.VCFMetadataLoader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.rand

class VCFSaverSuite extends PipelineBaseTest {
  val vcfPath = s"$testDataHome/joint/NA19625.chr20_18034651_18034655.g.vcf"

  def saveToDeltaAndMaybeVcf(
      vcf: String,
      header: VCFHeader,
      exportVCF: Boolean,
      exportVCFAsSingleFile: Boolean,
      vcfCompressionCodec: String): (DataFrame, String, String) = {

    val origDf = spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(vcf)

    val unsortedPartitionedDf = origDf.orderBy(rand(2019)).repartition(2)

    val outputDir = Files.createTempDirectory("vcfSaver")
    val deltaOutput = outputDir.resolve("deltaOutput").toString
    val vcfOutput = outputDir.resolve("vcfOutput").toString

    VCFSaver.saveToDeltaAndMaybeVCF(
      VCFHeaderWriter.writeHeaderAsString(header),
      unsortedPartitionedDf,
      exportVCF,
      exportVCFAsSingleFile,
      deltaOutput,
      None,
      vcfOutput,
      vcfCompressionCodec
    )

    (origDf, deltaOutput, vcfOutput)
  }

  test("Export partition-sorted VCF") {
    val matchingHeader = VCFMetadataLoader
      .readVcfHeader(spark.sparkContext.hadoopConfiguration, vcfPath)

    val (_, _, vcfOutput) = saveToDeltaAndMaybeVcf(
      vcfPath,
      matchingHeader,
      exportVCF = true,
      exportVCFAsSingleFile = false,
      vcfCompressionCodec = ""
    )

    val outputVcfList = new File(vcfOutput).listFiles.map(_.getName).filter(_.endsWith(".vcf"))
    assert(outputVcfList.length >= 1)

    val rereadDf = spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(vcfOutput)

    val rereadSortedWithinPartitions = rereadDf.sortWithinPartitions("contigName", "start")

    rereadDf.collect.zip(rereadSortedWithinPartitions.collect).foreach {
      case (vc1, vc2) =>
        assert(vc1 == vc2)
    }
  }

  test("Export single, sorted, compressed VCF") {
    val matchingHeader = VCFMetadataLoader
      .readVcfHeader(spark.sparkContext.hadoopConfiguration, vcfPath)

    val (origDf, _, vcfOutput) = saveToDeltaAndMaybeVcf(
      vcfPath,
      matchingHeader,
      exportVCF = true,
      exportVCFAsSingleFile = true,
      vcfCompressionCodec = "bgzf"
    )

    val savedVcf = VCFSaver.compressedVcfFileName(
      spark.sparkContext.hadoopConfiguration,
      "bgzf",
      vcfOutput
    )
    require(Files.isRegularFile(Paths.get(savedVcf)))

    val rereadDf = spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(savedVcf)

    assert(rereadDf.rdd.getNumPartitions == 1)
    val sortedOrigDf = origDf.orderBy("contigName", "start")
    rereadDf.collect.zip(sortedOrigDf.collect).foreach {
      case (vc1, vc2) =>
        assert(vc1 == vc2)
    }
  }

  test("Override header") {
    val mismatchingVcf = s"$testDataHome/joint/HG00096.chr20_18034651_18034655.g.vcf"
    val mismatchingHeader = VCFMetadataLoader
      .readVcfHeader(spark.sparkContext.hadoopConfiguration, mismatchingVcf)

    val (_, _, vcfOutput) = saveToDeltaAndMaybeVcf(
      vcfPath,
      mismatchingHeader,
      exportVCF = true,
      exportVCFAsSingleFile = true,
      vcfCompressionCodec = ""
    )

    val rereadHeader = VCFMetadataLoader
      .readVcfHeader(spark.sparkContext.hadoopConfiguration, vcfOutput)
    assert(rereadHeader.getGenotypeSamples.asScala == Seq("HG00096"))

    val rereadDf = spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(vcfOutput)

    val filteredRereadDf = rereadDf
      .filter("size(genotypes) = 1")
      .filter("genotypes[0].sampleId = 'HG00096'")
      .filter("genotypes[0].calls = array(-1, -1)")

    assert(rereadDf.count == filteredRereadDf.count)
  }
}
