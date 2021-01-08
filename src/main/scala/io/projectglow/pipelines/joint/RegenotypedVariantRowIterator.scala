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

package io.projectglow.pipelines.joint

import java.util.{NoSuchElementException, ArrayList => JArrayList, HashSet => JHashSet, TreeMap => JTreeMap}

import scala.collection.JavaConverters._
import htsjdk.samtools.util.Locatable
import htsjdk.variant.variantcontext.{Allele, GenotypeBuilder, GenotypeLikelihoods, VariantContext, VariantContextBuilder}
import htsjdk.variant.vcf.VCFHeader
import io.projectglow.common.logging.HlsUsageLogging
import io.projectglow.pipelines.sql.HLSConf
import io.projectglow.vcf.{InternalRowToVariantContextConverter, VariantContextToInternalRowConverter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.commons.collections4.Transformer
import org.apache.commons.collections4.map.LazyMap
import org.broadinstitute.hellbender.engine.{FeatureContext, ReferenceContext, ReferenceDataSource}
import org.broadinstitute.hellbender.tools.walkers.{GenotypeGVCFsEngine, ReferenceConfidenceVariantContextMerger}
import org.broadinstitute.hellbender.utils.SimpleInterval
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils

/**
 * From an iterator of sorted variant rows, creates an iterator of regenotyped rows.
 *
 * For each bin (containing all overlapping variant rows), the iterator steps through the rows,
 * keeping track of the locus (currentLocusBinDupOpt) for which it builds up the most up-to-date
 * context (currentSampleToVcsMap). The context is maintained across loci, as samples can have
 * rows spanning multiple loci.
 *
 * If a new locus is encountered, the current state is flushed: the current locus is regenotyped
 * with the relevant variant contexts. If the locus is non-null and non-duplicate, it is returned by
 * the iterator. A locus is duplicate if it is not in the bin; the associated row is included
 * in the iterator so that the regenotyper maintains the correct state regarding upstream spanning
 * deletions or reference blocks.
 *
 * @param rowBinDupIter Internal rows that must be sorted by (contigName, binId, start). Contain
 *                      metadata (bin ID, whether a variant row is duplicated) and can be converted
 *                      to variant contexts.
 */
private[joint] class RegenotypedVariantRowIterator(
    rowBinDupIter: Iterator[InternalRow],
    schema: StructType,
    header: VCFHeader,
    htsjdkVcConverter: InternalRowToVariantContextConverter,
    internalRowConverter: VariantContextToInternalRowConverter,
    gvcfEngine: GenotypeGVCFsEngine,
    merger: ReferenceConfidenceVariantContextMerger,
    referenceDataSource: ReferenceDataSource)
    extends Iterator[InternalRow]
    with HlsUsageLogging {

  // Current state
  var currentLocusBinDupOpt: Option[BinnedLocusMaybeDuplicated] = None
  var currentRegenotypedVcOpt: Option[VariantContext] = None

  val samples: Array[String] = header.getGenotypeSamples.asScala.toArray
  val currentSampleToVcsMap = new JTreeMap[String, JArrayList[VariantContext]]()
  samples.foreach { s =>
    currentSampleToVcsMap.put(s, new JArrayList[VariantContext]())
  }

  // Lazily-populated map from sample to dummy VariantContexts that do not overlap with real data
  val defaultTransformer: Transformer[String, VariantContext] =
    new Transformer[String, VariantContext] {
      val defaultAlleles = new JArrayList[Allele]()
      defaultAlleles.add(Allele.REF_N)
      val defaultVcb = new VariantContextBuilder("Unknown", "default_contig", 0, 0, defaultAlleles)
      def transform(sample: String): VariantContext = {
        val gt = GenotypeBuilder.createMissing(sample, 2)
        defaultVcb.genotypes(gt).make
      }
    }
  val defaultSampleToVcMap: LazyMap[String, VariantContext] =
    LazyMap.lazyMap(new JTreeMap[String, VariantContext](), defaultTransformer)

  // Metadata getters from InternalRows
  val binIdCol: Int = schema.fieldIndex("binId")
  val isDuplicateCol: Int = schema.fieldIndex("isDuplicate")
  def getBinId(r: InternalRow): Int = r.getInt(binIdCol)
  def getIsDuplicate(r: InternalRow): Boolean = r.getBoolean(isDuplicateCol)

  /**
   * Filters a list of variant contexts to those that overlap with the current locus.
   */
  private def filterRelevantVcs(vcs: JArrayList[VariantContext]): JArrayList[VariantContext] = {
    require(currentLocusBinDupOpt.isDefined)
    val locusStart = currentLocusBinDupOpt.get.locus.getStart
    val relevantVcs = new JArrayList[VariantContext](vcs.size)
    val vcIter = vcs.iterator
    while (vcIter.hasNext) {
      val vc = vcIter.next
      if (vc.getEnd >= locusStart) {
        relevantVcs.add(vc)
      }
    }
    relevantVcs
  }

  /**
   * Get variant contexts to regenotype based on the current state.
   *
   * Duplicates code from the GATK's ReferenceConfidenceVariantContextMerger.merge() function to
   * check if there are too many alternate alleles.
   * TODO: Remove referenceAlleleOpt and uniqueAlts logic when
   * https://github.com/broadinstitute/gatk/issues/6962 is resolved.
   *
   * @return None if there are no real alternate alleles, or if there are too many to regenotype.
   *         Otherwise, Some list of variant contexts.
   */
  private def getVariantContextsToRegenotype: Option[JArrayList[VariantContext]] = {
    require(currentLocusBinDupOpt.isDefined)
    val locusBinDup = currentLocusBinDupOpt.get

    val referenceAlleleOpt = getReferenceAllele
    if (referenceAlleleOpt.isEmpty) {
      // No alternate alleles
      return None
    }

    val uniqueAlts = new JHashSet[Allele](samples.length)
    val allEvents = new JArrayList[VariantContext](samples.length)

    val iter = currentSampleToVcsMap.entrySet.iterator
    while (iter.hasNext) {
      val entry = iter.next
      val sample = entry.getKey
      val vcs = filterRelevantVcs(entry.getValue)
      if (vcs.isEmpty) {
        // No overlapping events; propagate a dummy NON_REF variant context
        allEvents.add(defaultSampleToVcMap.get(sample))
      } else {
        // Overlapping events
        allEvents.addAll(vcs)
        addAlternateAlleles(vcs, referenceAlleleOpt.get, uniqueAlts)
      }
    }

    // Check if there are too many alternate alleles to regenotype
    val maxNumAlts = SQLConf.get.getConf(HLSConf.JOINT_GENOTYPING_MAX_NUM_ALT_ALLELES)
    if (uniqueAlts.size > maxNumAlts) {
      logger.warn(
        s"Skipping ${locusBinDup.locus}: has ${uniqueAlts.size} alternate alleles " +
        s"(maximum: $maxNumAlts)"
      )
      return None
    }

    Some(allEvents)
  }

  /**
   * Finds the longest reference allele from the relevant set of variant contexts.
   * Also checks that at least one variant context has a real alternate allele (not NON_REF).
   *
   * @return None if there are no real alternate alleles; Some reference allele otherwise.
   */
  private def getReferenceAllele: Option[Allele] = {
    require(currentLocusBinDupOpt.isDefined)
    val locusBinDup = currentLocusBinDupOpt.get

    var locusHasAlternateAlleles = false
    val relevantVcs = new JArrayList[VariantContext](samples.length)

    val vcsIter = currentSampleToVcsMap.values.iterator
    while (vcsIter.hasNext) {
      val vcs = filterRelevantVcs(vcsIter.next)
      // Overlapping events
      relevantVcs.addAll(vcs)
      if (!locusHasAlternateAlleles) {
        val vcIter = vcs.iterator
        while (vcIter.hasNext) {
          if (GATKVariantContextUtils.isProperlyPolymorphic(vcIter.next)) {
            locusHasAlternateAlleles = true
          }
        }
      }
    }
    if (locusHasAlternateAlleles) {
      Some(GATKVariantContextUtils.determineReferenceAllele(relevantVcs, locusBinDup.locus))
    } else {
      None
    }
  }

  /**
   * Adds real alternate alleles from a list of variant contexts that start at the same locus.
   *
   * If the variant context spans the locus and represents a deletion, adds a SPAN_DEL.
   * If the variant context starts at the same locus, remaps the alleles to match the reference
   * allele at the locus and filters for real alternate alleles.
   *
   * @param vcs Variant contexts with alternate alleles to collect
   * @param referenceAllele The longest reference allele at the current locus
   * @param alternateAlleles The collection of alternate alleles to add to
   */
  private def addAlternateAlleles(
      vcs: JArrayList[VariantContext],
      referenceAllele: Allele,
      alternateAlleles: JHashSet[Allele]): Unit = {
    require(currentLocusBinDupOpt.isDefined)

    val vcIter = vcs.iterator
    while (vcIter.hasNext) {
      val vc = vcIter.next
      // All variant contexts associated with a sample in the map start at the same locus
      // (see addVcToState)
      if (vc.getStart < currentLocusBinDupOpt.get.locus.getStart) {
        // Spanning event: duplicates logic from
        // ReferenceConfidenceVariantContextMerger.replaceWithNoCallsAndDels()
        val altAlleleIter = vc.getAlternateAlleles.iterator
        while (altAlleleIter.hasNext) {
          if (altAlleleIter.next.length < vc.getReference.length) {
            alternateAlleles.add(Allele.SPAN_DEL)
          }
        }
      } else {
        // Non-spanning event: duplicates logic from
        // ReferenceConfidenceVariantContextMerger.remapAlleles() and filterAllelesForFinalSet()
        val alleleIter = ReferenceConfidenceVariantContextMerger
          .remapAlleles(vc, referenceAllele)
          .iterator
        while (alleleIter.hasNext) {
          val allele = alleleIter.next
          if (allele != Allele.NON_REF_ALLELE &&
            !allele.isReference &&
            !(allele.isSymbolic && vc.isSymbolic) &&
            allele.isCalled) {
            alternateAlleles.add(allele)
          }
        }
      }
    }
  }

  /**
   * Regenotypes this site if there are any true alternate alleles in overlapping variant contexts.
   * If the locus is non-duplicate and the regenotyped variant context is non-null, the regenotyped
   * VC is updated.
   */
  private def regenotypeCurrentLocus(): Option[VariantContext] = {
    val relevantVariantContextsOpt = getVariantContextsToRegenotype
    if (relevantVariantContextsOpt.isEmpty) {
      return None
    }

    // The genotyper is stateful and holds on to previous indels, so we must run it on all
    // variant contexts in a bin (whether it will be output here or not).
    require(currentLocusBinDupOpt.isDefined)
    val locusBinDup = currentLocusBinDupOpt.get
    val referenceContext = new ReferenceContext(
      referenceDataSource,
      new SimpleInterval(locusBinDup.locus)
    )
    val featureContext = new FeatureContext()

    // Defaults for tlodThreshold and afTolerance are lifted from GenotypeGVCFs
    val regenotypedVc = gvcfEngine.callRegion(
      locusBinDup.locus,
      relevantVariantContextsOpt.get,
      referenceContext,
      featureContext,
      merger,
      /* somaticInput */ false,
      /* tlodThreshold */ 3.5,
      /* afTolerance */ 1e-3
    )

    if (locusBinDup.isDuplicate) {
      // If this locus was regenotyped for context padding only, it will be output elsewhere.
      return None
    }

    // The regenotyped VariantContext may be null if the site turned monomorphic.
    Option(regenotypedVc)
  }

  /**
   * If there is no regenotyped variant row queued up, steps through the rows until either the
   * iterator is exhausted or a regenotyped row is queued up.
   *
   * When a row from the iterator identifies a new variant locus or bin, the current locus is
   * regenotyped with the most up-to-date row per sample (if there was a locus before) and the
   * existing locus is replaced. If this is a new bin, the row map is also emptied as each bin
   * has its own state.
   *
   * Each row encountered is also added to the current state.
   */
  private def setNextRegenotypedVc(): Unit = {
    while (rowBinDupIter.hasNext && currentRegenotypedVcOpt.isEmpty) {
      val newRowBinDup = rowBinDupIter.next
      htsjdkVcConverter.convert(newRowBinDup).foreach { newVc =>
        val newLocusBinDup = BinnedLocusMaybeDuplicated(
          Locus(newVc.getContig, newVc.getStart),
          getBinId(newRowBinDup),
          getIsDuplicate(newRowBinDup)
        )

        currentLocusBinDupOpt match {
          case None => // First locus only
            currentLocusBinDupOpt = Some(newLocusBinDup)
            resetMap()
          case Some(currentLocusBinDup) if currentLocusBinDup != newLocusBinDup =>
            // New locus or bin (isDuplicate is the same if locus/bin are the same): process the locus
            currentRegenotypedVcOpt = regenotypeCurrentLocus()
            currentLocusBinDupOpt = Some(newLocusBinDup)
            if (currentLocusBinDup.binId != newLocusBinDup.binId ||
              currentLocusBinDup.locus.contigName != newLocusBinDup.locus.contigName) {
              // New bin or contig: reset the VCF state
              resetMap()
            }
          case _ => // Same locus as before
        }

        addVcToState(newVc)
      }
    }

    // Process final locus
    if (!rowBinDupIter.hasNext && currentRegenotypedVcOpt.isEmpty &&
      currentLocusBinDupOpt.isDefined) {
      currentRegenotypedVcOpt = regenotypeCurrentLocus()
      currentLocusBinDupOpt = None
    }
  }

  /**
   * If a sample is associated with variant contexts at this locus (due to biallelic splitting), the
   * new VC is appended to the current sequence. Otherwise, the VCs are replaced.
   */
  private[joint] def addVcToState(newVc: VariantContext): Unit = {
    val currentVcs = currentSampleToVcsMap.get(newVc.getGenotypes.get(0).getSampleName)

    if (currentVcs == null) {
      throw new IllegalArgumentException(
        "Variant row does not contain samples provided in manifest."
      )
    }

    if (!currentVcs.isEmpty &&
      (currentVcs.get(0).getContig != newVc.getContig ||
      currentVcs.get(0).getStart != newVc.getStart)) {
      currentVcs.clear()
    }
    currentVcs.add(newVc)
  }

  /**
   * Replace all of the variant contexts associated with a sample. Should happen upon encountering a
   * new bin or contig.
   */
  private[joint] def resetMap(): Unit = {
    samples.foreach { s =>
      currentSampleToVcsMap.get(s).clear()
    }
  }

  override def hasNext(): Boolean = {
    setNextRegenotypedVc()
    currentRegenotypedVcOpt.isDefined
  }

  override def next(): InternalRow = {
    if (hasNext()) {
      val regenotypedVc = currentRegenotypedVcOpt.get
      currentRegenotypedVcOpt = None
      internalRowConverter.convertRow(regenotypedVc, false)
    } else {
      throw new NoSuchElementException("Iterator has no more regenotyped VCF rows.")
    }
  }
}

case class Locus(contigName: String, start: Long) extends Locatable {
  def getContig: String = contigName
  def getStart: Int = start.toInt
  def getEnd: Int = getStart
  override def toString: String = s"$getContig:$getStart"
}

// Includes information for binning duplication.
case class BinnedLocusMaybeDuplicated(locus: Locus, binId: Int, isDuplicate: Boolean)
