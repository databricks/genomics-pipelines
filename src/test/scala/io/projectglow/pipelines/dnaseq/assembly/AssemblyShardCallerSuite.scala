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

import htsjdk.samtools.{SAMFileHeader, ValidationStringency}
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFHeader
import io.projectglow.pipelines.PipelineBaseTest
import org.apache.spark.sql.functions._
import org.bdgenomics.adam.converters.AlignmentConverter
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.broadinstitute.hellbender.engine.filters.ReadFilter
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.ReferenceConfidenceMode
import org.broadinstitute.hellbender.transformers.ReadTransformer
import org.broadinstitute.hellbender.utils.downsampling.{PositionalDownsampler, ReadsDownsampler}
import org.broadinstitute.hellbender.utils.read.{GATKRead, SAMRecordToGATKReadAdapter}
import org.apache.spark.sql.functions._

class AssemblyShardCallerSuite extends PipelineBaseTest {

  val path = s"$testDataHome/NA12878_21_10002403.bam"

  def alignments: AlignmentDataset = {
    spark.sparkContext.loadAlignments(path, stringency = ValidationStringency.LENIENT)
  }

  def samHeader: SAMFileHeader = {
    alignments.convertToSam()._1
  }

  def getBaseReads: Iterator[GATKRead] = {
    val sess = spark
    import sess.implicits._

    val converter = new AlignmentConverter()
    val (header, _) = alignments.convertToSam()
    alignments
      .dataset
      .withColumn("alignment", struct("*"))
      .withColumn("sampleId", lit("NA12878"))
      .withColumn("binId", lit(1))
      .withColumn("binStart", lit(0))
      .withColumn("binEnd", lit(100000))
      .where($"readMapped")
      .as[BinnedAlignment]
      .orderBy($"alignment.referenceName", $"alignment.start", $"alignment.end")
      .collect()
      .iterator
      .map(
        r =>
          new SAMRecordToGATKReadAdapter(
            converter.convert(r.alignment.toAvro, header, alignments.readGroups)
          )
      )
  }

  def preReadFilterTransformerImpl: ReadTransformer = {
    new ReadTransformer {
      override def apply(read: GATKRead): GATKRead = {
        val transformedRead = read.copy()
        transformedRead.setAttribute("pr", "transformed")
        transformedRead
      }
    }
  }

  def readFilterImpl: ReadFilter = {
    new ReadFilter {
      override def test(read: GATKRead): Boolean = {
        if (read.hasAttribute("pr")) {
          read.getMappingQuality <= 30
        } else {
          read.getMappingQuality > 30
        }
      }
    }
  }

  def postReadFilterTransformerImpl: ReadTransformer = {
    new ReadTransformer {
      override def apply(read: GATKRead): GATKRead = {
        val transformedRead = read.copy()
        transformedRead.setAttribute("po", "transformed")
        transformedRead
      }
    }
  }

  def downsamplerImpl: ReadsDownsampler = {
    new PositionalDownsampler(1, samHeader)
  }

  class AssemblyShardCallerNoop extends AssemblyShardCaller {
    override def call(assemblyShard: AssemblyShard): Iterator[VariantContext] = Iterator.empty
    override def referenceConfidenceMode: ReferenceConfidenceMode = ReferenceConfidenceMode.NONE
    override def getVCFHeader: VCFHeader = new VCFHeader()
  }

  test("no transformations") {
    val caller = new AssemblyShardCallerNoop()
    val transformedIterator = caller.transformReads(getBaseReads)
    assert(transformedIterator.toSeq == getBaseReads.toSeq)
  }

  test("pre-filter transformer") {
    class AssemblyShardCallerImpl extends AssemblyShardCallerNoop {
      override def preReadFilterTransformer: Option[ReadTransformer] =
        Some(preReadFilterTransformerImpl)
    }
    val caller = new AssemblyShardCallerImpl()
    val transformedReads = caller.transformReads(getBaseReads)
    val expectedReads = getBaseReads.toSeq
    expectedReads.foreach(_.setAttribute("pr", "transformed"))
    assert(transformedReads.toSeq == expectedReads)
  }

  test("filter") {
    class AssemblyShardCallerImpl extends AssemblyShardCallerNoop {
      override def readFilter: Option[ReadFilter] = Some(readFilterImpl)
    }
    val caller = new AssemblyShardCallerImpl()
    val transformedReads = caller.transformReads(getBaseReads)
    val expectedReads = getBaseReads.filter(_.getMappingQuality > 30)
    assert(transformedReads.toSeq == expectedReads.toSeq)
  }

  test("post-filter transformer") {
    class AssemblyShardCallerImpl extends AssemblyShardCallerNoop {
      override def postReadFilterTransformer: Option[ReadTransformer] =
        Some(postReadFilterTransformerImpl)
    }
    val caller = new AssemblyShardCallerImpl()
    val transformedReads = caller.transformReads(getBaseReads)
    val expectedReads = getBaseReads.toSeq
    expectedReads.foreach(_.setAttribute("po", "transformed"))
    assert(transformedReads.toSeq == expectedReads)
  }

  test("downsampler") {
    class AssemblyShardCallerImpl extends AssemblyShardCallerNoop {
      override def downsampler: Option[ReadsDownsampler] = Some(downsamplerImpl)
    }

    val caller = new AssemblyShardCallerImpl()
    val transformedReads = caller.transformReads(getBaseReads)
    val numExpectedReads = getBaseReads.toSeq.map(_.getStart).distinct.size
    assert(transformedReads.size == numExpectedReads)
  }

  test("all transformations") {
    class AssemblyShardCallerImpl extends AssemblyShardCallerNoop {
      override def preReadFilterTransformer: Option[ReadTransformer] =
        Some(preReadFilterTransformerImpl)
      override def readFilter: Option[ReadFilter] = Some(readFilterImpl)
      override def postReadFilterTransformer: Option[ReadTransformer] =
        Some(postReadFilterTransformerImpl)
      override def downsampler: Option[ReadsDownsampler] = Some(downsamplerImpl)
    }

    val caller = new AssemblyShardCallerImpl()
    val transformedReads = caller.transformReads(getBaseReads).toSeq

    val expectedReads = getBaseReads.toSeq.flatMap { r =>
      r.setAttribute("pr", "transformed")
      if (r.getMappingQuality > 30) {
        None
      } else {
        r.setAttribute("po", "transformed")
        Some(r)
      }
    }

    val groupedTransformedReads = transformedReads.groupBy(_.getStart)
    val groupedExpectedReads = expectedReads.groupBy(_.getStart)
    assert(groupedTransformedReads.keys == groupedExpectedReads.keys)

    groupedTransformedReads.foreach {
      case (start, transformedReadsAtStart) =>
        assert(transformedReadsAtStart.size == 1)
        assert(groupedExpectedReads(start).contains(transformedReadsAtStart.head))
    }
  }
}
