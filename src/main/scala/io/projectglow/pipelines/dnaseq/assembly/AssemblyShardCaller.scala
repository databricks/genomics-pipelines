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

import htsjdk.samtools.SAMFileHeader
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFHeader
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.broadinstitute.barclay.argparser.ClassFinder
import org.broadinstitute.hellbender.engine.filters.ReadFilter
import org.broadinstitute.hellbender.tools.walkers.annotator.Annotation
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.ReferenceConfidenceMode
import org.broadinstitute.hellbender.transformers.ReadTransformer
import org.broadinstitute.hellbender.utils.downsampling.{ReadsDownsampler, ReadsDownsamplingIterator}
import org.broadinstitute.hellbender.utils.iterators.{ReadFilteringIterator, ReadTransformingIterator}
import org.broadinstitute.hellbender.utils.read.GATKRead

/**
 * Abstraction for a caller that can accept a shard of Assembly Reads
 * and call variants on it, producing an iterator of [[VariantContext]]
 */
trait AssemblyShardCaller extends Serializable {

  /**
   * Call Variants for this assembly shard.
   *
   * @param assemblyShard the shard for which variants need to be called
   * @return an iterator over the variants [[VariantContext]]
   */
  def call(assemblyShard: AssemblyShard): Iterator[VariantContext]

  def referenceConfidenceMode: ReferenceConfidenceMode

  /**
   * GATK callers have multiple stages of read pre-processing before assembly:
   * - Pre-filtering transformations
   * - Filtering
   * - Post-filtering transformations
   * - Downsampling
   *
   * Those pre-processing steps can be provided via their respective methods.
   */
  final def transformReads(originalReads: Iterator[GATKRead]): Iterator[GATKRead] = {
    val baseReads = originalReads.asJava

    val preReadTransformedIterator = if (preReadFilterTransformer.isDefined) {
      new ReadTransformingIterator(baseReads, preReadFilterTransformer.get)
    } else {
      baseReads
    }

    val readFilteredIterator = if (readFilter.isDefined) {
      new ReadFilteringIterator(preReadTransformedIterator, readFilter.get)
    } else {
      preReadTransformedIterator
    }

    val postReadTransformedIterator = if (postReadFilterTransformer.isDefined) {
      new ReadTransformingIterator(readFilteredIterator, postReadFilterTransformer.get)
    } else {
      readFilteredIterator
    }

    val downsampledIterator = if (downsampler.isDefined) {
      new ReadsDownsamplingIterator(postReadTransformedIterator, downsampler.get)
    } else {
      postReadTransformedIterator
    }

    downsampledIterator.asScala
  }

  def preReadFilterTransformer: Option[ReadTransformer] = None

  def readFilter: Option[ReadFilter] = None

  def postReadFilterTransformer: Option[ReadTransformer] = None

  def downsampler: Option[ReadsDownsampler] = None

  def destroy(): Unit = {}

  def getVCFHeader: VCFHeader
}

object AssemblyShardCaller {
  val ANNOTATOR_PACKAGE = "org.broadinstitute.hellbender.tools.walkers.annotator"
  def getAnnotations(
      superType: Class[_ <: Annotation],
      packageToSearch: String = ANNOTATOR_PACKAGE): Seq[Annotation] = {
    val finder = new ClassFinder()
    finder.find(packageToSearch, superType)
    finder.getConcreteClasses.asScala.map(_.newInstance().asInstanceOf[Annotation]).toSeq
  }
}

trait AssemblyShardCallerFactory extends Serializable {
  def getCaller(header: SAMFileHeader): AssemblyShardCaller
  def getOutputHeader(reads: AlignmentDataset): VCFHeader = {
    val (samHeader, _) = reads.convertToSam()
    getCaller(samHeader).getVCFHeader
  }
}
