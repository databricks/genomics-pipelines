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

import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.functions.lit
import org.bdgenomics.adam.converters.AlignmentConverter
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.adam.sql.Alignment
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.PairHMMLikelihoodCalculationEngine.PCRErrorModel
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.ReferenceConfidenceMode
import org.broadinstitute.hellbender.utils.read.{GATKRead, SAMRecordToGATKReadAdapter}
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.dnaseq.util.VariantCallerArgs

class Mutect2ShardCallerSuite extends PipelineBaseTest {
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
    val caller = new Mutect2ShardCaller(
      header,
      referenceGenomeFastaPath,
      vcArgs,
      Some("NA12878")
    )
    val transformedReads = caller.transformReads(reads.iterator).toSeq
    assert(transformedReads.length == 212)
  }
}
