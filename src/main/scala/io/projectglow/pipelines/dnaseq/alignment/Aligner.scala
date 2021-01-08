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

package io.projectglow.pipelines.dnaseq.alignment

import java.util.function.{Function => JFunction}

import htsjdk.samtools.{Cigar, CigarElement, CigarOperator, SAMFlag, SAMTagUtil, TextCigarCodec, TextTagCodec}
import io.projectglow.common.logging.HlsUsageLogging
import org.bdgenomics.adam.sql.{Alignment, Fragment}
import org.broadinstitute.hellbender.utils.BaseUtils
import org.broadinstitute.hellbender.utils.bwa.{BwaMemAligner, BwaMemAlignment, BwaMemAlignmentUtils, BwaMemIndex, BwaMemIndexCache}

case class AlignmentInput(
    readName: String,
    readInFragment: Int,
    sequence: Array[Byte],
    qualityScores: String)

trait Aligner {
  def pairedEndMode: Boolean
  def align(records: java.util.List[AlignmentInput]): java.util.List[Fragment]
}

/**
 * A wrapper around GATK's [[Aligner]] that handles conversion between their internal
 * format and ADAM's [[Fragment]].
 *
 * @param sampleName The sample name for which we're doing alignment. This sample name
 *                   will be included in all output fragments.
 * @param readGroup The read group for which we're doing alignment. This read group name will be
 *                  included in all output fragments.
 */
class BwaAligner(sampleName: String, readGroup: String, aligner: BwaMemAligner) extends Aligner {
  val tagCodec = new TextTagCodec()
  val tagUtil = new SAMTagUtil()

  override def pairedEndMode: Boolean = {
    val flag = aligner.getFlagOption
    (BwaMemAligner.MEM_F_PE | flag) == flag
  }

  def align(records: java.util.List[AlignmentInput]): java.util.List[Fragment] = {
    val output = new java.util.ArrayList[Fragment](records.size)
    val rawAlignments = aligner.alignSeqs(records, getSeq)
    var currentRead: String = null
    var currentReadInputs = new java.util.ArrayList[AlignmentInput](2) // Should have length two
    // Initialize with length 4 expecting 2 reads and up to 2 alignments per read
    var currentReadRecs = new java.util.ArrayList[java.util.List[BwaMemAlignment]](4)

    // Collect all alignment records for a given read, and then convert them into a Fragment
    // object
    var i = 0
    while (i < records.size()) {
      val unalignedRead = records.get(i)
      val rawAlignment = rawAlignments.get(i)
      if (currentRead == null) {
        currentRead = unalignedRead.readName
      } else if (unalignedRead.readName != currentRead) {
        output.add(makeFrag(currentRead, currentReadInputs, currentReadRecs))
        currentRead = unalignedRead.readName
        currentReadRecs = new java.util.ArrayList[java.util.List[BwaMemAlignment]](4)
        currentReadInputs = new java.util.ArrayList[AlignmentInput](2)
      }
      currentReadRecs.add(rawAlignment)
      currentReadInputs.add(unalignedRead)
      i += 1
    }

    if (!currentReadRecs.isEmpty) {
      output.add(makeFrag(currentRead, currentReadInputs, currentReadRecs))
    }
    output
  }

  /**
   * Create a fragment given the input reads and their corresponding alignment results.
   */
  private def makeFrag(
      readName: String,
      inputs: java.util.List[AlignmentInput],
      raw: java.util.List[java.util.List[BwaMemAlignment]]): Fragment = {
    var insertSize: Option[Long] = None
    var i = 0

    // The BWA library doesn't return the correct insert size, so we iterate through the
    // alignments to find a read from which we can compute the insert size. We'll then use it
    // to correctly set the insert size on all the primary alignments that are proper pairs.
    while (i < raw.size() && insertSize.isEmpty) {
      var j = 0
      val alignments = raw.get(i)
      while (j < alignments.size() && insertSize.isEmpty) {
        insertSize = getInferredInsertSize(alignments.get(j))
        j += 1
      }
      i += 1
    }

    val alignmentRecords = scala.collection.mutable.ListBuffer[Alignment]()
    i = 0
    while (i < inputs.size()) {
      var j = 0
      val input = inputs.get(i)
      val alignments = raw.get(i)
      val saTags =
        BwaMemAlignmentUtils.createSATags(alignments, aligner.getIndex.getReferenceContigNames)
      while (j < alignments.size()) {
        alignmentRecords.append(
          bwaToAlignmentRecord(
            input,
            alignments.get(j),
            insertSize,
            Option(saTags.getOrDefault(alignments.get(j), null))
          )
        )
        j += 1
      }
      i += 1
    }

    new Fragment(
      name = Option(readName),
      readGroupId = Option(readGroup),
      insertSize = insertSize.map(_.toInt),
      alignments = alignmentRecords
    )
  }

  val getSeq = new JFunction[AlignmentInput, Array[Byte]] {
    def apply(input: AlignmentInput) = input.sequence
  }

  /**
   * Construct ADAM's [[Alignment]] from the BWA alignment output and the original read.
   *
   * The insert size must be passed as a parameter since it cannot always be inferred.
   */
  private def bwaToAlignmentRecord(
      input: AlignmentInput,
      alignment: BwaMemAlignment,
      insertSizeOpt: Option[Long],
      saTag: Option[String]): Alignment = {
    val flag = alignment.getSamFlag
    val readInFrag = if (!pairedEndMode || SAMFlag.FIRST_OF_PAIR.isSet(flag)) 0 else 1
    val aligned = alignment.getRefId != -1
    val contigName = if (aligned) {
      Option(aligner.getIndex.getReferenceContigNames.get(alignment.getRefId))
    } else {
      None
    }

    val isNegative = SAMFlag.READ_REVERSE_STRAND.isSet(flag)
    val (seqBytes, qual) = if (isNegative) {
      (BaseUtils.simpleReverseComplement(input.sequence), input.qualityScores.reverse)
    } else {
      (input.sequence, input.qualityScores)
    }

    val (trimmedSeq, trimmedQual, cigar) = if (SAMFlag.SUPPLEMENTARY_ALIGNMENT.isSet(flag)) {
      (
        getSupplementalBases(alignment, seqBytes),
        getSupplementalQuality(alignment, qual),
        getSupplementalCigar(alignment.getCigar)
      )
    } else {
      (seqBytes, qual, alignment.getCigar)
    }

    val (startTrim, endTrim) = parseStartAndEndTrimFromCigar(cigar)
    val properPair = SAMFlag.PROPER_PAIR.isSet(flag)
    val inferredInsertSize = if (!properPair) {
      None
    } else if (alignment.getRefStart > alignment.getMateRefStart) {
      insertSizeOpt.map(_.unary_-)
    } else {
      insertSizeOpt
    }

    val attributes = buildSAMAttributes(alignment, saTag)
    val mateMapped = pairedEndMode && SAMFlag.MATE_UNMAPPED.isUnset(flag)

    Alignment(
      readInFragment = Option(readInFrag),
      referenceName = contigName,
      start = if (aligned) Option(alignment.getRefStart.toLong) else None,
      originalStart = None,
      end = if (aligned) Option(alignment.getRefEnd) else None,
      mappingQuality = if (aligned) Option(alignment.getMapQual) else None,
      readName = Option(input.readName),
      sequence = Option(new String(trimmedSeq, "ASCII")),
      qualityScores = Option(trimmedQual),
      cigar = Option(cigar),
      originalCigar = None,
      basesTrimmedFromStart = if (aligned) Option(startTrim) else Some(0),
      basesTrimmedFromEnd = if (aligned) Option(endTrim) else Some(0),
      readPaired = Option(SAMFlag.READ_PAIRED.isSet(flag)),
      properPair = Option(properPair),
      readMapped = Option(SAMFlag.READ_UNMAPPED.isUnset(flag)),
      mateMapped = Option(mateMapped),
      failedVendorQualityChecks = Option(SAMFlag.READ_FAILS_VENDOR_QUALITY_CHECK.isSet(flag)),
      duplicateRead = Option(SAMFlag.DUPLICATE_READ.isSet(flag)),
      readNegativeStrand = Option(isNegative),
      mateNegativeStrand = Option(SAMFlag.MATE_REVERSE_STRAND.isSet(flag)),
      primaryAlignment = Option(SAMFlag.SECONDARY_ALIGNMENT.isUnset(flag)),
      secondaryAlignment = Option(SAMFlag.SECONDARY_ALIGNMENT.isSet(flag)),
      supplementaryAlignment = Option(SAMFlag.SUPPLEMENTARY_ALIGNMENT.isSet(flag)),
      mismatchingPositions = Option(alignment.getMDTag),
      originalQualityScores = None,
      attributes = Option(attributes),
      readGroupId = Option(readGroup),
      readGroupSampleId = Option(sampleName),
      mateAlignmentStart = if (mateMapped) Option(alignment.getMateRefStart) else None,
      mateReferenceName = if (mateMapped) {
        Option(aligner.getIndex.getReferenceContigNames.get(alignment.getMateRefId))
      } else {
        None
      },
      insertSize = inferredInsertSize
    )
  }

  private def buildSAMAttributes(alignment: BwaMemAlignment, saTag: Option[String]): String = {
    val builder = new java.lang.StringBuilder()
    def write(key: String, value: Any): Unit = {
      builder.append(tagCodec.encode(key, value))
      builder.append('\t')
    }
    if (alignment.getRefId != -1) {
      write("XS", alignment.getSuboptimalScore)
      write("AS", alignment.getAlignerScore)
      write("NM", alignment.getNMismatches)
      write("RG", readGroup)
    } else {
      write("XS", 0)
      write("AS", 0)
      write("RG", readGroup)
    }
    if (alignment.getXATag != null) {
      write("XA", alignment.getXATag)
    }
    if (saTag.nonEmpty) {
      write("SA", saTag.get)
    }
    if (builder.length > 0) {
      builder.deleteCharAt(builder.length - 1) // Delete last tab
    }
    builder.toString
  }

  // Pulled from ADAM's [[SAMRecordConverter]]
  private def parseStartAndEndTrimFromCigar(cigar: String): (Int, Int) = {
    val startTrim = if (cigar == "*" || cigar.isEmpty) {
      0
    } else {
      val count = cigar.takeWhile(_.isDigit).toInt
      val operator = cigar.dropWhile(_.isDigit).head

      if (operator == 'H') {
        count
      } else {
        0
      }
    }
    val endTrim = if (cigar.endsWith("H")) {
      // must reverse string as takeWhile is not implemented in reverse direction
      cigar.dropRight(1).reverse.takeWhile(_.isDigit).reverse.toInt
    } else {
      0
    }
    (startTrim, endTrim)
  }

  private def getInferredInsertSize(alignment: BwaMemAlignment): Option[Long] = {
    val isPrimary = SAMFlag.SECONDARY_ALIGNMENT.isUnset(alignment.getSamFlag) &&
      SAMFlag.SUPPLEMENTARY_ALIGNMENT.isUnset(alignment.getSamFlag)
    val isProper = SAMFlag.PROPER_PAIR.isSet(alignment.getSamFlag)
    if (isPrimary && isProper && alignment.getRefStart > alignment.getMateRefStart) {
      Option(alignment.getRefEnd - alignment.getMateRefStart)
    } else {
      None
    }
  }

  // Borrowed from https://github.com/broadinstitute/gatk/blob/master/src/main/java/org/broadinstitute/hellbender/utils/bwa/BwaMemAlignmentUtils.java#L46
  private def getSupplementalCigar(cigarStr: String): String = {
    val cigar = TextCigarCodec.decode(cigarStr)
    if (cigar.getFirstCigarElement.getOperator == CigarOperator.S || cigar
        .getLastCigarElement
        .getOperator == CigarOperator.S) {
      val newCigar = new Cigar()
      val elements = cigar.getCigarElements
      var i = 0
      while (i < elements.size()) {
        val element = elements.get(i)
        if (element.getOperator == CigarOperator.S) {
          newCigar.add(new CigarElement(element.getLength, CigarOperator.H))
        } else {
          newCigar.add(element)
        }
        i += 1
      }
      TextCigarCodec.encode(newCigar)
    } else {
      cigarStr
    }
  }

  private def getSupplementalQuality(alignment: BwaMemAlignment, initialQual: String): String = {
    initialQual.slice(alignment.getSeqStart, alignment.getSeqEnd)
  }

  private def getSupplementalBases(
      alignment: BwaMemAlignment,
      initialBases: Array[Byte]): Array[Byte] = {
    initialBases.slice(alignment.getSeqStart, alignment.getSeqEnd)
  }
}

object BwaAligner extends HlsUsageLogging {
  def defaultForSample(
      indexPath: String,
      sampleName: String,
      readGroup: String,
      pairedEndMode: Boolean): BwaAligner = {
    val index = BwaMemIndexCache.getInstance(indexPath)
    val aligner = new BwaMemAligner(index)

    logger.warn(
      s"Using bwa version ${BwaMemIndex.getBWAVersion} with " +
      s"index $indexPath"
    )

    if (pairedEndMode) {
      aligner.alignPairs()
    }
    aligner.inferPairEndStats()
    aligner.setNThreadsOption(1)
    new BwaAligner(sampleName, readGroup, aligner)
  }
}
