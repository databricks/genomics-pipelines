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

import scala.collection.JavaConverters._
import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.functions.lit
import org.bdgenomics.adam.converters.AlignmentConverter
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.adam.sql.Alignment
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.PairHMMLikelihoodCalculationEngine.PCRErrorModel
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.ReferenceConfidenceMode
import org.broadinstitute.hellbender.utils.read.{GATKRead, SAMRecordToGATKReadAdapter}
import org.broadinstitute.hellbender.utils.SimpleInterval
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.dnaseq.util.VariantCallerArgs

case class ShardCallerTestRecord(sampleId: String, contigName: String, start: Int, end: Int)

class HaplotypeAssemblyShardCallerSuite extends PipelineBaseTest {
  val vcArgs = VariantCallerArgs(
    ReferenceConfidenceMode.NONE,
    PCRErrorModel.CONSERVATIVE,
    maxReadsPerAlignmentStart = None
  )

  private def getReads(
      alignments: AlignmentDataset,
      record: ShardCallerTestRecord): Seq[GATKRead] = {

    val sess = spark
    import sess.implicits._
    val converter = new AlignmentConverter()
    val (header, _) = alignments.convertToSam()
    alignments
      .dataset
      .withColumn("sampleId", lit(record.sampleId))
      .withColumn("binId", lit(1))
      .withColumn("binStart", lit(record.start))
      .withColumn("binEnd", lit(record.end))
      .as[Alignment]
      .orderBy("referenceName", "start", "end")
      .collect()
      .map(
        r =>
          new SAMRecordToGATKReadAdapter(converter.convert(r.toAvro, header, alignments.readGroups))
      )
  }

  private def assertCallsOnRegions(
      record: ShardCallerTestRecord,
      alignedReadsPath: String,
      expectedVariants: Long): Unit = {
    val path = s"$testDataHome/$alignedReadsPath"
    val ShardCallerTestRecord(sampleId, contigName, start, end) = record
    val bedPath = s"$testDataHome/${sampleId}_${contigName}_${start}_$end.bed"
    val targets = sparkContext.loadFeatures(bedPath)

    val alignedReads = sparkContext
      .loadAlignments(path, stringency = ValidationStringency.LENIENT)
      .transformDataset(ds => {
        new AssemblyReadsBuilder(ds)
          .filterReadsByQuality(20)
          .filterTargetedReads(targets)
          .build()
      })

    val iter = getReads(alignedReads, record).iterator
    val (header, _) = alignedReads.convertToSam()
    val intervals = List(new SimpleInterval(contigName, start, end))
    val shard = AssemblyShard(iter.asJava, intervals.head, intervals.asJava, intervals.asJava)
    val caller = new HaplotypeAssemblyShardCaller(header, referenceGenomeFastaPath, vcArgs)
    val variantContexts = caller.call(shard)

    assert(variantContexts.size == expectedVariants)
    caller.destroy()
  }

  val data = List(
    ShardCallerTestRecord("NA12878", "21", 10168300, 10168900),
    ShardCallerTestRecord("NA12878", "21", 10014930, 10151000)
  )

  gridTest("call")(data) { record =>
    val relativeBamPath = "big-files/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam"
    val vcfPath = s"$testDataHome/dnaseq/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
    val allVariants = spark.read.format("vcf").load(vcfPath)
    val expectedVariants = allVariants
      .filter(s"contigName = ${record.contigName}")
      .filter(s"start < ${record.end}")
      .filter(s"end > ${record.start}")
      .count()
    assertCallsOnRegions(record, relativeBamPath, expectedVariants)
  }

  test("filter reads") {
    val alignedReads = sparkContext
      .loadAlignments(
        s"$testDataHome/NA12878_20_10000117.bam",
        stringency = ValidationStringency.LENIENT
      )
    val record = ShardCallerTestRecord("NA12878", "20", 0, Integer.MAX_VALUE)
    val reads = getReads(alignedReads, record)
    assert(reads.length == 258) // Sanity check

    val (header, _) = alignedReads.convertToSam()
    val caller = new HaplotypeAssemblyShardCaller(header, referenceGenomeFastaPath, vcArgs)
    val transformedReads = caller.transformReads(reads.iterator).toSeq
    assert(transformedReads.length == 212)
  }
}
