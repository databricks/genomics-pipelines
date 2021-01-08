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

import java.nio.file.{Files, Paths}

import org.bdgenomics.adam.rdd.ADAMContext._
import io.projectglow.pipelines.NotebookParam
import io.projectglow.pipelines.{NotebookParam, PipelineBaseTest}

class DNASeqPipelineSuite extends PipelineBaseTest {

  private val stringType: String = classOf[String].getSimpleName
  private val intType: String = classOf[Int].getSimpleName
  private val booleanType: String = classOf[Boolean].getSimpleName

  private val baseArgs: Set[(String, Option[String], String)] = Set(
    ("manifest", None, stringType),
    ("output", None, stringType),
    ("replayMode", Some("skip"), stringType),
    ("perSampleTimeout", Some("12h"), stringType)
  )

  private val alignArgs: Set[(String, Option[String], String)] = Set(
    ("functionalEquivalence", Some("false"), booleanType),
    ("pairedEndMode", Some("true"), booleanType),
    ("markDuplicates", Some("false"), booleanType),
    ("recalibrateBaseQualities", Some("false"), booleanType),
    ("binQualityScores", Some("false"), booleanType),
    ("exportBam", Some("false"), booleanType),
    ("exportBamAsSingleFile", Some("true"), booleanType),
    ("knownSites", Some(""), stringType)
  )

  private val variantCallerArgs: Set[(String, Option[String], String)] = Set(
    ("maxReadStartsPerPosition", Some("50"), intType),
    ("targetedRegions", Some(""), stringType),
    ("excludedRegions", Some(""), stringType),
    ("referenceConfidenceMode", Some("NONE"), stringType),
    ("pcrIndelModel", Some("CONSERVATIVE"), stringType),
    ("minMappingQuality", Some("20"), intType),
    ("validationStringency", Some("STRICT"), stringType),
    ("exportVCF", Some("false"), booleanType),
    ("exportVCFAsSingleFile", Some("true"), booleanType),
    ("vcfCompressionCodec", Some("bgzf"), stringType)
  )

  private def checkParams(pipeline: DNASeqPipeline, runStages: RunStages): Unit = {

    val userVisibleParams = pipeline.getVisibleParams
    val actualUserVisibleNames = userVisibleParams.map {
      case NotebookParam(name, _, default, dataType, _) =>
        (name, default, dataType)
    }.toSet

    var expectedUserVisibleNames = baseArgs
    if (runStages.align) {
      expectedUserVisibleNames ++= alignArgs
    }
    if (runStages.callVariants) {
      expectedUserVisibleNames ++= variantCallerArgs
    }

    assert(
      expectedUserVisibleNames == actualUserVisibleNames,
      unionDiffIntersect(expectedUserVisibleNames, actualUserVisibleNames)
    )

    val requiredParams = pipeline.getVisibleParams.filter(_.defaultValue.isEmpty)
    val actualParamNames = requiredParams.map(_.name)
    val expectedParamNames = Seq("manifest", "output").sorted
    assert(actualParamNames sameElements expectedParamNames, (actualParamNames, expectedParamNames))
  }

  private def unionDiffIntersect[T](a: Set[T], b: Set[T]): Set[T] = {
    a.union(b).diff(a.intersect(b))
  }

  private val pipelineRunStages: Seq[(DNASeqPipeline, RunStages)] = Seq(
    (DNASeqPipeline, RunStages(align = true, callVariants = true)),
    (
      new DNASeqPipeline(align = true, callVariants = true),
      RunStages(align = true, callVariants = true)
    ),
    (
      new DNASeqPipeline(align = false, callVariants = true),
      RunStages(align = false, callVariants = true)
    ),
    (
      new DNASeqPipeline(align = true, callVariants = false),
      RunStages(align = true, callVariants = false)
    )
  )

  gridTest("check params")(pipelineRunStages) {
    case (pipeline, runStages) => checkParams(pipeline, runStages)
  }

  private val illegalStageCombinations: Seq[(RunStages, String)] = Seq(
    (RunStages(align = false, callVariants = false), "No stages to run")
  )

  gridTest("illegal customizations")(illegalStageCombinations) {
    case (runStages, errorMsg) =>
      val e = intercept[IllegalArgumentException] {
        new DNASeqPipeline(runStages.align, runStages.callVariants)
      }
      assert(e.getMessage.contains(errorMsg))
  }

  private def execute(pipeline: DNASeqPipeline, runStages: RunStages): Unit = {
    val manifestFile = if (runStages.align) {
      s"$testDataHome/raw.reads/GIAB.NA12878.20p12.1.manifest"
    } else {
      s"$testDataHome/aligned.reads/GIAB.NA12878.20p12.1.manifest"
    }

    val outputDir = Files.createTempDirectory("dnaseq").toAbsolutePath.toString
    val params = Map(
      "manifest" -> manifestFile,
      "output" -> outputDir,
      "referenceGenomeFastaPath" -> referenceGenomeFastaPath
    )

    pipeline.init(params)
    pipeline.execute(spark)

    val sampleId = "NA12878"

    val alignmentOutput = s"$outputDir/aligned/recordGroupSample=$sampleId"
    if (runStages.align) {
      val testAlignments = spark.sparkContext.loadAlignments(alignmentOutput).toDF.count()
      val goldenAlignments = spark
        .sparkContext
        .loadAlignments(
          s"$testDataHome/aligned.reads/bwa_mem_paired_end.bam"
        )
        .toDF
        .count()
      assert(testAlignments ~== goldenAlignments relTol 0.05)
    } else {
      assert(Files.notExists(Paths.get(alignmentOutput)))
    }

    val callVariantsOutput = s"$outputDir/genotypes/sampleId=$sampleId"
    lazy val goldenVariants = spark
      .read
      .format("vcf")
      .load(s"$testDataHome/dnaseq/aligned.reads.vcf")
      .count()

    if (runStages.callVariants) {
      val testVariants = spark.read.format("delta").load(callVariantsOutput).count()
      assert(testVariants == goldenVariants)
    } else {
      assert(Files.notExists(Paths.get(callVariantsOutput)))
    }
  }

  gridTest("execute")(pipelineRunStages) {
    case (pipeline, runStages) =>
      execute(pipeline, runStages)
  }
}

case class RunStages(align: Boolean, callVariants: Boolean)
