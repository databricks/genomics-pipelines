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

import scala.collection.JavaConversions._ // scalastyle:ignore
import scala.math.max
import htsjdk.samtools.{Cigar, CigarElement, CigarOperator, TextCigarCodec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}
import org.bdgenomics.adam.models.ReadGroupDictionary
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.sql.{Alignment, Fragment}
import org.bdgenomics.formats.avro.Strand
import io.projectglow.common.GlowLogging
import io.projectglow.pipelines.sql.HLSConf

object MarkDuplicates extends Serializable with GlowLogging {

  // A fragment with annotations for the library, left reference position, and right reference position.
  case class FragmentAnnotated(
      name: Option[String],
      readGroupId: Option[String],
      insertSize: Option[Int],
      alignments: Seq[Alignment],
      library: String,
      leftReferencePosition: ReferencePosition,
      rightReferencePosition: ReferencePosition,
      score: Int)

  // A genomic locus represented by the name of the reference contig, the position, and the strand.
  case class ReferencePosition(referenceName: String, pos: Long, orientation: String)

  // An annotated fragment and its score.
  case class FragmentScore(fragment: FragmentAnnotated, score: Int)

  // Calculates the unclipped 5' position for an alignment record.
  private def fivePrimePosition(r: Row): Long = {
    lazy val samtoolsCigar: Cigar = {
      TextCigarCodec.decode(r.getAs[String]("cigar"))
    }
    def isClipped(el: CigarElement) = {
      el.getOperator == CigarOperator.SOFT_CLIP ||
      el.getOperator == CigarOperator.HARD_CLIP
    }
    lazy val unclippedEnd: Long = {
      max(
        0L,
        samtoolsCigar
          .getCigarElements
          .reverse
          .takeWhile(isClipped)
          .foldLeft(r.getAs[Long]("end"))({ (pos, cigarEl) =>
            pos + cigarEl.getLength
          })
      )
    }
    lazy val unclippedStart: Long = {
      max(
        0L,
        samtoolsCigar
          .getCigarElements
          .takeWhile(isClipped)
          .foldLeft(r.getAs[Long]("start"))({ (pos, cigarEl) =>
            pos - cigarEl.getLength
          })
      )
    }
    if (r.getAs[Boolean]("readNegativeStrand")) unclippedEnd else unclippedStart
  }

  // Constructs the reference position for an alignment record.
  private def getPos(r: Row): ReferencePosition = {
    if (r.getAs[Boolean]("readMapped")) {
      val orientation = if (r.getAs[Boolean]("readNegativeStrand")) {
        Strand.REVERSE.toString
      } else {
        Strand.FORWARD.toString
      }
      ReferencePosition(r.getAs[String]("referenceName"), fivePrimePosition(r), orientation)
    } else {
      ReferencePosition(r.getAs[String]("sequence"), 0L, Strand.INDEPENDENT.toString)
    }
  }

  // Splits a set of rows of alignment records into primary mapped, secondary mapped, and unmapped alignment records.
  private def getMappedRows(reads: Seq[Row]): (Seq[Row], Seq[Row], Seq[Row]) = {
    val (mapped, unmapped) = reads.partition(_.getAs[Boolean]("readMapped"))
    val (primaryMapped, secondaryMapped) = mapped.partition(_.getAs[Boolean]("primaryAlignment"))
    (primaryMapped, secondaryMapped, unmapped)
  }

  // Splits a set of alignment records into primary mapped, secondary mapped, and unmapped alignment records.
  private def getMappedRecords(
      reads: Seq[Alignment]): (Seq[Alignment], Seq[Alignment], Seq[Alignment]) = {

    val (mapped, unmapped) = reads.partition(_.readMapped.getOrElse(false))
    val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment.getOrElse(false))
    (primaryMapped, secondaryMapped, unmapped)
  }

  // Calculates the sum of the phred scores that are greater than or equal to 15.
  def getReadScore(qualityScore: Option[String]): Int = {
    qualityScore.getOrElse("").toCharArray.map(c => c - 33).filter(_ >= 15).sum
  }

  // Constructs an alignment record from an original and sets its duplicate flag.
  private def markDuplicateReads(r: Alignment, isDup: Boolean): Alignment = {
    r.copy(duplicateRead = Option(isDup))
  }

  // Marks the reads in a fragment as duplicates based on their alignments.
  private def markDuplicateFragment(
      fragment: FragmentAnnotated,
      primaryAreDups: Boolean,
      secondaryAreDups: Boolean,
      unmappedAreDups: Boolean): Fragment = {

    val (primaryMapped, secondaryMapped, unmapped) = getMappedRecords(fragment.alignments)
    val markedAlignments = primaryMapped.map(r => markDuplicateReads(r, primaryAreDups)) ++
      secondaryMapped.map(r => markDuplicateReads(r, secondaryAreDups)) ++
      unmapped.map(r => markDuplicateReads(r, unmappedAreDups))

    Fragment(
      fragment.name,
      fragment.readGroupId,
      fragment.insertSize,
      markedAlignments
    )
  }

  /**
   * Marks duplicates fragments in the underlying iterator.
   *
   * The underlying iterator is assumed to be sorted by:
   * - Library
   * - Left reference position, which is never null
   * - Right reference position, nulls last
   * - Score, descending
   */
  class DuplicateMarkingIterator(it: Iterator[FragmentAnnotated]) extends Iterator[Fragment] {
    private var currentLibrary: String = _
    private var currentLeft: ReferencePosition = _
    private var currentRight: ReferencePosition = _
    private var seenPairForCurrentLeft = false

    override def hasNext: Boolean = it.hasNext

    override def next(): Fragment = {
      val nextAnnotated = it.next()
      if (nextAnnotated.leftReferencePosition == null) {
        // Unmapped read, can't be duplicate
        markDuplicateFragment(
          nextAnnotated,
          primaryAreDups = false,
          secondaryAreDups = false,
          unmappedAreDups = false
        )
      } else if (nextAnnotated.library == currentLibrary && nextAnnotated.leftReferencePosition == currentLeft) {
        val nextRight = nextAnnotated.rightReferencePosition
        if (nextRight == null && seenPairForCurrentLeft) {
          // Some fragments with the current left position are proper pairs, so unpaired reads
          // should be marked as duplicates
          markDuplicateFragment(
            nextAnnotated,
            primaryAreDups = true,
            secondaryAreDups = true,
            unmappedAreDups = false
          )
        } else if (nextRight == currentRight) {
          // If we've already seen a fragment with the same right position, it was higher scored
          // than the current read, so this is a duplicate
          markDuplicateFragment(
            nextAnnotated,
            primaryAreDups = true,
            secondaryAreDups = true,
            unmappedAreDups = false
          )
        } else {
          // If this is the first fragment with this right position, it's the highest scoring
          // fragment and not a duplicate
          currentRight = nextRight
          markDuplicateFragment(
            nextAnnotated,
            primaryAreDups = false,
            secondaryAreDups = true,
            unmappedAreDups = false
          )
        }
      } else {
        // New library or left position
        currentLibrary = nextAnnotated.library
        currentLeft = nextAnnotated.leftReferencePosition
        currentRight = nextAnnotated.rightReferencePosition
        seenPairForCurrentLeft = currentRight != null
        markDuplicateFragment(
          nextAnnotated,
          primaryAreDups = false,
          secondaryAreDups = true,
          unmappedAreDups = false
        )
      }
    }
  }

  // Get the library for a fragment.
  private def getLibrary(rgd: ReadGroupDictionary) =
    udf[String, Seq[Row]]((reads: Seq[Row]) => {
      val recordGroupName = Option(reads.head.getAs[String]("readGroupId"))
      recordGroupName.flatMap(name => rgd(name).library).orNull
    })

  /**
   * Gets the reference position for a given strand orientation. Handles cases where reads are
   * reversed during alignment.
   * @param defaultPos The expected reference position for this strand. This position is chosen
   *                   if it has the desired orientation or if neither position has the desired
   *                   orientation.
   * @param otherPos The other possible reference position for this strand. This position is chosen
   *                 if it is the only position with the correct orientation.
   * @param strand The desired orientation
   */
  private def positionForStrand(
      defaultPos: Option[ReferencePosition],
      otherPos: Option[ReferencePosition],
      strand: Strand): Option[ReferencePosition] = {
    defaultPos
      .filter(_.orientation == strand.toString)
      .orElse(otherPos.filter(_.orientation == strand.toString).orElse(defaultPos))
  }

  case class PositionsAndScore(
      left: Option[ReferencePosition],
      right: Option[ReferencePosition],
      score: Int)
  // Get the start position pair for the primary aligned first and second of pair reads as well
  // as the score for this fragment.
  private val getReferencePositionAndScore = udf[PositionsAndScore, Seq[Row]]((reads: Seq[Row]) => {
    val (primaryMapped, _, unmapped) = getMappedRows(reads)
    val firstOfPair = (primaryMapped.filter(_.getAs[Int]("readInFragment") == 0) ++
    unmapped.filter(_.getAs[Int]("readInFragment") == 0)).toSeq
    val secondOfPair = (primaryMapped.filter(_.getAs[Int]("readInFragment") == 1) ++
    unmapped.filter(_.getAs[Int]("readInFragment") == 1)).toSeq
    val score = primaryMapped
      .filter(r => !r.getAs[Boolean]("supplementaryAlignment"))
      .map(r => getReadScore(Option(r.getAs[String]("qualityScores"))))
      .sum

    if (firstOfPair.size + secondOfPair.size > 0) {
      val initialLeft = firstOfPair.headOption.map(getPos)
      val initialRight = secondOfPair.headOption.map(getPos)
      PositionsAndScore(
        positionForStrand(initialLeft, initialRight, Strand.FORWARD),
        positionForStrand(initialRight, initialLeft, Strand.REVERSE),
        score
      )
    } else {
      PositionsAndScore((primaryMapped ++ unmapped).headOption.map(getPos), None, score)
    }
  })

  // Print warning message to the user if there are record groups without library names, as these will be treated as
  // coming from a single library.
  private def checkReadGroups(rgd: ReadGroupDictionary) {
    val emptyRgs = rgd
      .readGroups
      .filter(_.library.isEmpty)

    emptyRgs.foreach(rg => {
      logger.warn(
        "Library ID is empty for record group %s from sample %s.".format(rg.id, rg.sampleId)
      )
    })

    if (emptyRgs.nonEmpty) {
      logger.warn(
        "For duplicate marking, all reads whose library is unknown will be treated as coming from the same library."
      )
    }
  }

  /**
   * Based on ADAM's [[org.bdgenomics.adam.rdd.read.MarkDuplicates]], but uses Spark SQL for
   * partitioning and sorting.
   *
   * The primary motivators for this implementation vs the one in ADAM:
   * - It's faster to sort and shuffle Spark SQL's rows than normal Java objects
   * - ADAM uses [[org.apache.spark.rdd.RDD.groupBy()]], which requires that all values for any
   * given key fit in memory. If there are many duplicates, this requirement can lead to memory
   * pressure and OOMs.
   */
  def markDuplicateFragments(fragments: FragmentDataset): FragmentDataset = {
    checkReadGroups(fragments.readGroups)

    fragments.transformDataset { ds =>
      import ds.sparkSession.implicits._
      val rgd = fragments.readGroups
      val numPartitions = ds.sparkSession.conf.get(HLSConf.MARK_DUPLICATES_NUM_PARTITIONS.key).toInt

      ds.withColumn("library", getLibrary(rgd)(col("alignments")))
        // Annotate each fragment with its left and right reference positions as well as the
        // score
        .withColumn("positionsAndScore", getReferencePositionAndScore(col("alignments")))
        .withColumn("leftReferencePosition", col("positionsAndScore.left"))
        .withColumn("rightReferencePosition", col("positionsAndScore.right"))
        .withColumn("score", col("positionsAndScore.score"))
        .repartition(numPartitions, col("library"), col("leftReferencePosition"))
        .sortWithinPartitions(
          col("library"),
          col("leftReferencePosition"),
          col("rightReferencePosition").asc_nulls_last,
          col("score").desc
        )
        .as[FragmentAnnotated]
        .mapPartitions(it => new DuplicateMarkingIterator(it))
    }
  }
}
