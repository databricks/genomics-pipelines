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

import java.nio.file.Files

import io.projectglow.pipelines.PipelineBaseTest
import org.apache.spark.sql.functions._

class VCFReaderSuite extends PipelineBaseTest {
  val vcfPath = s"$testDataHome/dnaseq/NA12878_21_10002403.vcf"

  test("convert delta path with partition to where clause") {
    val outputDir = Files.createTempDirectory("vcfreadersuite").toAbsolutePath.toString
    val vcfDf = spark
      .read
      .format("vcf")
      .load(vcfPath)
      .withColumn("sampleId", lit("sample"))

    vcfDf
      .write
      .partitionBy("sampleId")
      .format("delta")
      .save(outputDir)

    val (_, rereadDf) = VCFReader.readFromDeltaOrVCF(spark, outputDir + "/sampleId=sample")
    assert(rereadDf.count() == vcfDf.count())
  }

  test("convert delta path with partition to where clause (empty partition)") {
    val outputDir = Files.createTempDirectory("vcfreadersuite").toAbsolutePath.toString
    val vcfDf = spark
      .read
      .format("vcf")
      .load(vcfPath)
      .withColumn("sampleId", lit("sample"))

    vcfDf
      .write
      .partitionBy("sampleId")
      .format("delta")
      .save(outputDir)

    intercept[IllegalArgumentException] {
      VCFReader.readFromDeltaOrVCF(spark, outputDir + "/sampleId=monkey")
    }
  }

  test("read from VCF when path points to a VCF") {
    val fromVcfDirect = spark.read.format("vcf").load(vcfPath)
    val (_, fromDeltaOrVcf) = VCFReader.readFromDeltaOrVCF(spark, vcfPath)
    assert(fromVcfDirect.count() == fromDeltaOrVcf.count())
  }
}
