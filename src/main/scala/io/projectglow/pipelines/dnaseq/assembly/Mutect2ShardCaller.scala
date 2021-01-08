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

import java.io.ByteArrayOutputStream
import java.nio.file.Paths

import scala.collection.JavaConverters._
import htsjdk.samtools.SAMFileHeader
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriterBuilder}
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}
import io.projectglow.common.logging.HlsUsageLogging
import io.projectglow.vcf.VCFHeaderUtils
import org.broadinstitute.hellbender.engine._
import org.broadinstitute.hellbender.engine.filters.ReadFilter
import org.broadinstitute.hellbender.tools.walkers.annotator._
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.{HaplotypeCaller, ReferenceConfidenceMode}
import org.broadinstitute.hellbender.tools.walkers.mutect.{M2ArgumentCollection, Mutect2Engine}
import org.broadinstitute.hellbender.transformers.{PalindromeArtifactClipReadTransformer, ReadTransformer}
import org.broadinstitute.hellbender.utils.downsampling.{MutectDownsampler, ReadsDownsampler}
import org.broadinstitute.hellbender.utils.smithwaterman.SmithWatermanAligner.Implementation
import io.projectglow.pipelines.dnaseq.util.IntervalUtil
import io.projectglow.pipelines.dnaseq.util.{IntervalUtil, VariantCallerArgs}

case class Mutect2ArgumentCollection(optNormalSampleName: Option[String])
    extends M2ArgumentCollection {
  // Since GATK 4.1, any non-normal samples from the SAM header are inferred as tumor samples
  normalSamples = optNormalSampleName.toList.asJava
}

/**
 * Implementation of [[AssemblyShardCaller]] for calling mutations using GATK's [[Mutect2Engine]]
 *
 * @param header The SAM File Header
 * @param referenceGenomeIndexPath Path to the reference genome index file
 * @param variantCallerArgs User provided arguments passed to the variant caller
 */
class Mutect2ShardCaller(
    header: SAMFileHeader,
    referenceGenomeIndexPath: String,
    variantCallerArgs: VariantCallerArgs,
    optNormalSample: Option[String])
    extends AssemblyShardCaller
    with HlsUsageLogging {

  private val referenceDataSource = ReferenceDataSource.of(Paths.get(referenceGenomeIndexPath))

  private val m2Args = Mutect2ArgumentCollection(optNormalSample)
  m2Args.likelihoodArgs.pcrErrorModel = variantCallerArgs.pcrIndelModel
  m2Args.smithWatermanImplementation = Implementation.FASTEST_AVAILABLE

  private val annotator = new VariantAnnotatorEngine(
    AssemblyShardCaller.getAnnotations(classOf[StandardMutectAnnotation]).asJava,
    null,
    Nil.asJava,
    /* useRaw */ false,
    /* keepCombined */ false
  )

  private val m2 = new Mutect2Engine(
    m2Args,
    /* createBamOutIndex */ false,
    /* createBamOutMD5 */ false,
    header,
    referenceGenomeIndexPath,
    annotator
  )

  override def readFilter: Option[ReadFilter] = {
    val filters = Mutect2Engine.makeStandardMutect2ReadFilters()
    Some(ReadFilter.fromList(filters, header))
  }

  override def postReadFilterTransformer: Option[ReadTransformer] = {
    Some(
      new PalindromeArtifactClipReadTransformer(
        referenceDataSource,
        Mutect2Engine.MIN_PALINDROME_SIZE
      )
    )
  }

  override def downsampler: Option[ReadsDownsampler] = {
    variantCallerArgs.maxReadsPerAlignmentStart.map {
      new MutectDownsampler(
        _,
        m2Args.maxSuspiciousReadsPerAlignmentStart,
        m2Args.downsamplingStride
      )
    }
  }

  override def call(assemblyShard: AssemblyShard): Iterator[VariantContext] = {
    val ai = new AssemblyRegionIterator(
      assemblyShard,
      header,
      referenceDataSource,
      /* features */ null,
      m2,
      HaplotypeCaller.DEFAULT_MIN_ASSEMBLY_REGION_SIZE,
      HaplotypeCaller.DEFAULT_MAX_ASSEMBLY_REGION_SIZE,
      HaplotypeCaller.DEFAULT_ASSEMBLY_REGION_PADDING,
      HaplotypeCaller.DEFAULT_ACTIVE_PROB_THRESHOLD,
      HaplotypeCaller.DEFAULT_MAX_PROB_PROPAGATION_DISTANCE,
      /* includeReadsWithDeletionsInIsActivePileups */ true
    ).asScala

    val emptyFeatureContext = new FeatureContext()

    ai.flatMap { ar =>
      m2.callRegion(
          ar,
          new ReferenceContext(referenceDataSource, ar.getExtendedSpan),
          emptyFeatureContext
        )
        .asScala
        .filter { vc =>
          IntervalUtil.pointInInterval(vc.getStart, assemblyShard.callableRegion)
        }
    }
  }

  override def getVCFHeader: VCFHeader = {
    val stream = new ByteArrayOutputStream()
    val writer = new VariantContextWriterBuilder()
      .clearOptions()
      .setOutputStream(stream)
      .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
      .setOption(Options.WRITE_FULL_FORMAT_FIELD)
      .build
    m2.writeHeader(writer, Set.empty[VCFHeaderLine].asJava)
    VCFHeaderUtils.parseHeaderFromString(stream.toString)
  }

  override def destroy(): Unit = {
    m2.shutdown()
  }

  override def referenceConfidenceMode: ReferenceConfidenceMode = ReferenceConfidenceMode.NONE
}

class Mutect2ShardCallerFactory(
    referenceGenomeIndexPath: String,
    variantCallerArgs: VariantCallerArgs,
    optNormalSample: Option[String])
    extends AssemblyShardCallerFactory {

  override def getCaller(header: SAMFileHeader): AssemblyShardCaller = {
    new Mutect2ShardCaller(
      header,
      referenceGenomeIndexPath,
      variantCallerArgs,
      optNormalSample
    )
  }
}
