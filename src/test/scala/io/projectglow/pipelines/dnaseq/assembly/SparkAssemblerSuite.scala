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

package io.projectglow.pipelines.dnaseq.assembly

import java.nio.file.Files

import scala.collection.JavaConverters._
import htsjdk.samtools.{SAMFileHeader, ValidationStringency}
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.variantcontext.writer.VCFHeaderWriter
import htsjdk.variant.vcf.VCFHeader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hls.dsl.expressions.overlaps
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureDataset
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.adam.sql.Genotype
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.PairHMMLikelihoodCalculationEngine.PCRErrorModel
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.ReferenceConfidenceMode
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.dnaseq.util.VariantCallerArgs

class SparkAssemblerSuite extends PipelineBaseTest {

  test("calls") {

    val testRecords = List(
      (
        "big-files/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam",
        "NA12878_21_10168300_10168900.bed",
        "CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
      ),
      (
        "big-files/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam",
        "NA12878_21_10014930_10151000.bed",
        "CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
      )
    )

    for ((alignedReadsRelPath, bedRelPath, truthVCFRelPath) <- testRecords) {
      val path = s"$testDataHome/$alignedReadsRelPath"

      val alignedReads =
        sparkContext.loadAlignments(path, stringency = ValidationStringency.LENIENT)
      this.assertCallsOnAlignedReads(alignedReads, truthVCFRelPath, bedRelPath)
    }
  }

  test("call unsorted alignments") {
    val testRecords = List(
      (
        "big-files/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam",
        "NA12878_21_10014930_10151000.bed",
        "CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
      )
    )

    for ((alignedReadsRelPath, bedRelPath, truthVCFRelPath) <- testRecords) {
      val path = s"$testDataHome/$alignedReadsRelPath"

      val alignedReads = sparkContext
        .loadAlignments(path, stringency = ValidationStringency.LENIENT)
        .transformDataset(ds => {
          // sorting by desc start position breaks the sort by contig, start asc that we expect
          import ds.sparkSession.implicits._
          ds.sort($"start".desc)
        })
      this.assertCallsOnAlignedReads(alignedReads, truthVCFRelPath, bedRelPath)
    }
  }

  test("assembly shard calls are banded in gvcf mode") {
    val input = s"$bigFilesHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam"
    val reads = sparkContext.loadAlignments(input, stringency = ValidationStringency.LENIENT)
    val bedPath = s"$testDataHome/NA12878_21_10014930_10151000.bed"
    val targetRegions = sparkContext.loadFeatures(bedPath)
    val bandedRdd = callVariants(reads, Some(targetRegions), ReferenceConfidenceMode.GVCF)._2
    val unbandedRdd =
      callVariants(reads, Some(targetRegions), ReferenceConfidenceMode.BP_RESOLUTION)._2
    assert(bandedRdd.rdd.count() < unbandedRdd.rdd.count())
  }

  test("pcrIndelModel is passed to HaplotypeCaller") {
    val input = s"$bigFilesHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam"
    val reads = sparkContext.loadAlignments(input, stringency = ValidationStringency.LENIENT)
    val conservative = callVariants(reads, None, pcrIndelModel = PCRErrorModel.CONSERVATIVE)._2
    val none = callVariants(reads, None, pcrIndelModel = PCRErrorModel.NONE)._2
    // Should have more indels called when we don't correct for PCR
    assert(none.rdd.count() > conservative.rdd.count())
  }

  private def callVariants(
      alignedReads: AlignmentDataset,
      targetRegionsOpt: Option[FeatureDataset],
      referenceConfidenceMode: ReferenceConfidenceMode = ReferenceConfidenceMode.NONE,
      pcrIndelModel: PCRErrorModel = PCRErrorModel.CONSERVATIVE): (VCFHeader, DataFrame) = {

    // apply the basic transformations to prepare the aligned reads
    val transformedReads = alignedReads.transformDataset(ds => {
      val builder = new AssemblyReadsBuilder(ds.coalesce(1))
        .filterReadsByQuality(20)
      targetRegionsOpt.map(builder.filterTargetedReads).getOrElse(builder).build()
    })
    val vcArgs = VariantCallerArgs(
      referenceConfidenceMode,
      pcrIndelModel,
      maxReadsPerAlignmentStart = Some(50)
    )
    val factory = new HaplotypeShardCallerFactory(referenceGenomeFastaPath, vcArgs)
    val variantRows = SparkAssembler.shardAndCallVariants(
      transformedReads,
      factory,
      optTargets = targetRegionsOpt
    )
    val header = factory.getOutputHeader(transformedReads)
    (header, variantRows)
  }

  private def assertCallsOnAlignedReads(
      alignedReads: AlignmentDataset,
      truthVCFRelPath: String,
      bedRelPath: String): Unit = {

    val outputDir = Files.createTempDirectory("output")
    val ourCallsPath = outputDir.resolve("ourCalls.vcf").toString
    val truthVCFPath = s"$testDataHome/dnaseq/$truthVCFRelPath"
    val bedPath = s"$testDataHome/$bedRelPath"
    val targetRegions = sparkContext.loadFeatures(bedPath)

    val (vcfHeader, variantRows) = callVariants(alignedReads, Some(targetRegions))
    val vcfHeaderStr = VCFHeaderWriter.writeHeaderAsString(vcfHeader)

    variantRows
      .write
      .format("bigvcf")
      .option("vcfHeader", vcfHeaderStr)
      .save(ourCallsPath)

    VariantContextConverter.setNestAnnotationInGenotypesProperty(
      sparkContext.hadoopConfiguration,
      true
    )

    val gatkCalls = sparkContext
      .loadGenotypes(truthVCFPath)
      .transformDataset(ds => {
        import ds.sqlContext.implicits._
        val targets = targetRegions.dataset
        ds.coalesce(1)
          .join(
            targets,
            ds("referenceName") === targets("referenceName") &&
            overlaps(ds("start"), ds("end"), targets("start"), targets("end")),
            "left_semi"
          )
          .orderBy($"referenceName", $"start", $"end")
          .as[Genotype]
      })

    val ourCalls = sparkContext
      .loadGenotypes(ourCallsPath, stringency = ValidationStringency.LENIENT)
      .transformDataset(ds => {
        import ds.sqlContext.implicits._
        ds.coalesce(1).orderBy($"referenceName", $"start", $"end")
      })

    val difference = diff(gatkCalls.dataset, ourCalls.dataset)

    assert(difference.count() == 0, difference.show())

    assert(ourCalls.dataset.count === gatkCalls.dataset.count)
  }

  test("reads are sorted and include SAM attributes") {
    val reads = sparkContext
      .loadAlignments(
        s"$bigFilesHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam",
        stringency = ValidationStringency.LENIENT
      )
      .transformDataset(_.where("readMapped").limit(1000))

    // No error expected
    SparkAssembler.shardAndCallVariants(reads, new TestFactory(), None, None).collect()
  }
}

class TestCaller extends AssemblyShardCaller {
  override def call(assemblyShard: AssemblyShard): Iterator[VariantContext] = {
    val list = assemblyShard.iterator.asScala.toList
    list.foreach { r =>
      assert(r.hasAttribute("RG"))
    }
    val sorted = list.sortBy(r => (r.getAssignedStart, r.getName, r.isSecondOfPair))
    assert(sorted == list, s"${sorted.zip(list).mkString("\n")}")
    Iterator.empty
  }

  override def referenceConfidenceMode: ReferenceConfidenceMode = ReferenceConfidenceMode.NONE

  override def getVCFHeader: VCFHeader = new VCFHeader()
}

class TestFactory extends AssemblyShardCallerFactory {
  override def getCaller(header: SAMFileHeader): AssemblyShardCaller = new TestCaller()
}
