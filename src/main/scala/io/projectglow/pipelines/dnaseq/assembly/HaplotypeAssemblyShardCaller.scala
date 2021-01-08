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

import java.nio.file.Paths

import htsjdk.samtools.SAMFileHeader
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFConstants
import org.broadinstitute.hellbender.engine._
import org.broadinstitute.hellbender.tools.walkers.annotator._
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.{HaplotypeCallerArgumentCollection, HaplotypeCallerEngine, ReferenceConfidenceMode => Mode}
import org.broadinstitute.hellbender.utils.fasta.CachingIndexedFastaSequenceFile

import scala.collection.JavaConverters._
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine, VCFStandardHeaderLines}
import org.broadinstitute.hellbender.engine.filters.ReadFilter
import org.broadinstitute.hellbender.utils.downsampling.{PositionalDownsampler, ReadsDownsampler}
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.HaplotypeCaller
import org.broadinstitute.hellbender.utils.smithwaterman.SmithWatermanAligner.Implementation
import org.broadinstitute.hellbender.utils.variant.{GATKVCFConstants, GATKVCFHeaderLines}
import io.projectglow.pipelines.dnaseq.util.IntervalUtil
import io.projectglow.pipelines.dnaseq.util.{IntervalUtil, VariantCallerArgs}

/**
 * Implementation of [[AssemblyShardCaller]] for calling variants using GATK's
 * [[HaplotypeCallerEngine]]
 *
 * @param header The SAM File Header
 * @param referenceGenomeIndexPath Path to the reference genome index file
 * @param variantCallerArgs User provided arguments passed to the variant caller
 */
class HaplotypeAssemblyShardCaller(
    header: SAMFileHeader,
    referenceGenomeIndexPath: String,
    variantCallerArgs: VariantCallerArgs)
    extends AssemblyShardCaller {

  private val referenceDataSource = ReferenceDataSource.of(Paths.get(referenceGenomeIndexPath))

  // the GATK Haplotype Caller is not serializable and is expensive to instantiate
  // so make sure it is instantiated exactly once per partition
  private val hcArgs = new HaplotypeCallerArgumentCollection()
  hcArgs.emitReferenceConfidence = variantCallerArgs.referenceConfidenceMode
  hcArgs.likelihoodArgs.pcrErrorModel = variantCallerArgs.pcrIndelModel
  hcArgs.smithWatermanImplementation = Implementation.FASTEST_AVAILABLE

  private val baseAnnotations = AssemblyShardCaller.getAnnotations(classOf[StandardAnnotation]) ++
    AssemblyShardCaller.getAnnotations(classOf[StandardHCAnnotation])
  private val annotations = if (referenceConfidenceMode != Mode.NONE) {
    HaplotypeCallerEngine.filterReferenceConfidenceAnnotations(
      new java.util.ArrayList(baseAnnotations.asJava)
    )
  } else {
    baseAnnotations.asJava
  }

  private val annotatorEngine = new VariantAnnotatorEngine(
    annotations,
    hcArgs.dbsnp.dbsnp,
    hcArgs.comps,
    hcArgs.emitReferenceConfidence != Mode.NONE,
    /* keepCombined */ false
  )

  private val hc = new HaplotypeCallerEngine(
    hcArgs,
    /* createBamOutIndex */ false,
    /* createBamOutMD5 */ false,
    header,
    new CachingIndexedFastaSequenceFile(Paths.get(referenceGenomeIndexPath)),
    annotatorEngine
  )

  override def readFilter: Option[ReadFilter] = {
    val filters = HaplotypeCallerEngine.makeStandardHCReadFilters()
    Some(ReadFilter.fromList(filters, header))
  }

  override def downsampler: Option[ReadsDownsampler] = {
    variantCallerArgs.maxReadsPerAlignmentStart.map {
      new PositionalDownsampler(_, header)
    }
  }

  override def call(assemblyShard: AssemblyShard): Iterator[VariantContext] = {
    val ai = new AssemblyRegionIterator(
      assemblyShard,
      header,
      referenceDataSource,
      /* features */ null,
      hc,
      HaplotypeCaller.DEFAULT_MIN_ASSEMBLY_REGION_SIZE,
      HaplotypeCaller.DEFAULT_MAX_ASSEMBLY_REGION_SIZE,
      HaplotypeCaller.DEFAULT_ASSEMBLY_REGION_PADDING,
      HaplotypeCaller.DEFAULT_ACTIVE_PROB_THRESHOLD,
      HaplotypeCaller.DEFAULT_MAX_PROB_PROPAGATION_DISTANCE,
      /* includeReadsWithDeletionsInIsActivePileups */ true
    ).asScala

    val emptyFeatureContext = new FeatureContext()

    ai.flatMap { ar =>
      hc.callRegion(ar, emptyFeatureContext)
        .asScala
        .filter { vc =>
          IntervalUtil.pointInInterval(vc.getStart, assemblyShard.callableRegion)
        }
    }
  }

  override def getVCFHeader: VCFHeader = {
    val vcfHeader = hc.makeVCFHeader(header.getSequenceDictionary, Set.empty[VCFHeaderLine].asJava)
    if (hcArgs.emitReferenceConfidence == Mode.GVCF) {
      // Header lines added by [[org.broadinstitute.hellbender.utils.variant.writer.GVCFWriter]]
      vcfHeader.addMetaDataLine(VCFStandardHeaderLines.getInfoLine(VCFConstants.END_KEY))
      vcfHeader.addMetaDataLine(
        GATKVCFHeaderLines.getFormatLine(GATKVCFConstants.MIN_DP_FORMAT_KEY)
      )
    }
    vcfHeader
  }

  override def destroy(): Unit = {
    hc.shutdown()
  }

  override def referenceConfidenceMode: Mode = variantCallerArgs.referenceConfidenceMode
}

class HaplotypeShardCallerFactory(
    referenceGenomeIndexPath: String,
    variantCallerArgs: VariantCallerArgs)
    extends AssemblyShardCallerFactory {

  override def getCaller(header: SAMFileHeader): AssemblyShardCaller = {
    new HaplotypeAssemblyShardCaller(header, referenceGenomeIndexPath, variantCallerArgs)
  }
}
