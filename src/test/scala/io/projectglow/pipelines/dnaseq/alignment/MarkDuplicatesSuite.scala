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

import java.util.UUID

import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.PipelineBaseTest
import org.bdgenomics.adam.models.{ReadGroup, ReadGroupDictionary, SequenceDictionary}
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.formats.avro.{Alignment, Reference}

/**
 * MarkDuplicatesSuite from ADAM
 */
class MarkDuplicatesSuite extends PipelineBaseTest {

  val rgd = new ReadGroupDictionary(
    Seq(new ReadGroup("sammy sample", "machine foo", library = Some("library bar")))
  )

  def createUnmappedRead(): Alignment = {
    Alignment
      .newBuilder()
      .setReadMapped(false)
      .setSequence("ACGT")
      .build()
  }

  def createMappedRead(
      referenceName: String,
      start: Long,
      end: Long,
      readName: String = UUID.randomUUID().toString,
      avgPhredScore: Int = 20,
      numClippedBases: Int = 0,
      isPrimaryAlignment: Boolean = true,
      isNegativeStrand: Boolean = false): Alignment = {
    assert(avgPhredScore >= 10 && avgPhredScore <= 50)
    val qual = (for (i <- 0 until 100) yield (avgPhredScore + 33).toChar).toString()
    val cigar =
      if (numClippedBases > 0) "%dS%dM".format(numClippedBases, 100 - numClippedBases) else "100M"

    val reference = Reference
      .newBuilder
      .setName(referenceName)
      .build

    Alignment
      .newBuilder()
      .setReferenceName(reference.getName)
      .setStart(start)
      .setQualityScores(qual)
      .setCigar(cigar)
      .setEnd(end)
      .setReadMapped(true)
      .setPrimaryAlignment(isPrimaryAlignment)
      .setReadName(readName)
      .setReadGroupId("machine foo")
      .setDuplicateRead(false)
      .setReadNegativeStrand(isNegativeStrand)
      .setSequence("ACGT")
      .build()
  }

  def createPair(
      firstReferenceName: String,
      firstStart: Long,
      firstEnd: Long,
      secondReferenceName: String,
      secondStart: Long,
      secondEnd: Long,
      readName: String = UUID.randomUUID().toString,
      avgPhredScore: Int = 20): Seq[Alignment] = {
    val firstReference = Reference
      .newBuilder
      .setName(firstReferenceName)
      .build

    val secondReference = Reference
      .newBuilder
      .setName(secondReferenceName)
      .build

    val firstOfPair = createMappedRead(
      firstReferenceName,
      firstStart,
      firstEnd,
      readName = readName,
      avgPhredScore = avgPhredScore
    )
    firstOfPair.setReadInFragment(0)
    firstOfPair.setMateMapped(true)
    firstOfPair.setMateReferenceName(secondReference.getName)
    firstOfPair.setMateAlignmentStart(secondStart)
    firstOfPair.setReadPaired(true)
    val secondOfPair = createMappedRead(
      secondReferenceName,
      secondStart,
      secondEnd,
      readName = readName,
      avgPhredScore = avgPhredScore,
      isNegativeStrand = true
    )
    secondOfPair.setReadInFragment(1)
    secondOfPair.setMateMapped(true)
    secondOfPair.setMateReferenceName(firstReference.getName)
    secondOfPair.setMateAlignmentStart(firstStart)
    secondOfPair.setReadPaired(true)
    Seq(firstOfPair, secondOfPair)
  }

  private def markDuplicates(reads: Alignment*): Array[Alignment] = {

    MarkDuplicates
      .markDuplicateFragments(
        AlignmentDataset(
          spark.sparkContext.parallelize(reads),
          SequenceDictionary.empty,
          rgd,
          Seq.empty
        ).toFragments
      )
      .toAlignments()
      .rdd
      .collect()
  }

  test("single read") {
    val read = createMappedRead("0", 100, 200)
    val marked = markDuplicates(read)
    // Can't have duplicates with a single read, should return the read unchanged.
    assert(marked(0) == read)
  }

  test("reads at different positions") {
    val read1 = createMappedRead("0", 42, 142)
    val read2 = createMappedRead("0", 43, 143)
    val marked = markDuplicates(read1, read2)
    // Reads shouldn't be modified
    assert(marked.contains(read1) && marked.contains(read2))
  }

  test("reads at the same position") {
    val poorReads = for (i <- 0 until 10) yield {
      createMappedRead("1", 42, 142, avgPhredScore = 20, readName = "poor%d".format(i))
    }
    val bestRead = createMappedRead("1", 42, 142, avgPhredScore = 30, readName = "best")
    val marked = markDuplicates(List(bestRead) ++ poorReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("reads at the same position with clipping") {
    val poorClippedReads = for (i <- 0 until 5) yield {
      createMappedRead(
        "1",
        44,
        142,
        numClippedBases = 2,
        avgPhredScore = 20,
        readName = "poorClipped%d".format(i)
      )
    }
    val poorUnclippedReads = for (i <- 0 until 5) yield {
      createMappedRead("1", 42, 142, avgPhredScore = 20, readName = "poorUnclipped%d".format(i))
    }
    val bestRead = createMappedRead("1", 42, 142, avgPhredScore = 30, readName = "best")
    val marked = markDuplicates(List(bestRead) ++ poorClippedReads ++ poorUnclippedReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("reads on reverse strand") {
    val poorReads = for (i <- 0 until 7) yield {
      createMappedRead(
        "10",
        42,
        142,
        isNegativeStrand = true,
        avgPhredScore = 20,
        readName = "poor%d".format(i)
      )
    }
    val bestRead = createMappedRead(
      "10",
      42,
      142,
      isNegativeStrand = true,
      avgPhredScore = 30,
      readName = "best"
    )
    val marked = markDuplicates(List(bestRead) ++ poorReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("unmapped reads") {
    val unmappedReads = for (i <- 0 until 10) yield createUnmappedRead()
    val marked = markDuplicates(unmappedReads: _*)
    assert(marked.size == unmappedReads.size)
    // Unmapped reads should never be marked duplicates
    assert(marked.forall(p => !p.getDuplicateRead))
  }

  test("read pairs") {
    val poorPairs =
      for {
        i <- 0 until 10
        read <- createPair(
          "0",
          10,
          110,
          "0",
          110,
          210,
          avgPhredScore = 20,
          readName = "poor%d".format(i)
        )
      } yield read
    val bestPair = createPair("0", 10, 110, "0", 110, 210, avgPhredScore = 30, readName = "best")
    val marked = markDuplicates(bestPair ++ poorPairs: _*)
    val (dups, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == "best"))
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("read pairs with fragments") {
    val fragments = for (i <- 0 until 10) yield {
      createMappedRead("2", 33, 133, avgPhredScore = 40, readName = "fragment%d".format(i))
    }
    // Even though the phred score is lower, pairs always score higher than fragments
    val pairs = createPair("2", 33, 133, "2", 100, 200, avgPhredScore = 20, readName = "pair")
    val marked = markDuplicates(fragments ++ pairs: _*)
    val (dups, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == "pair"))
    assert(dups.size == 10 && dups.forall(p => p.getReadName.startsWith("fragment")))
  }

  test("quality scores") {
    // The ascii value 53 is equal to a phred score of 20
    val qual = 53.toChar.toString * 100
    assert(MarkDuplicates.getReadScore(Option(qual)) == 2000)
  }

  test("read pairs that cross chromosomes") {
    val poorPairs =
      for {
        i <- 0 until 10
        read <- createPair(
          "ref0",
          10,
          110,
          "ref1",
          110,
          210,
          avgPhredScore = 20,
          readName = "poor%d".format(i)
        )
      } yield read
    val bestPair =
      createPair("ref0", 10, 110, "ref1", 110, 210, avgPhredScore = 30, readName = "best")
    val marked = markDuplicates(bestPair ++ poorPairs: _*)
    val (dups, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == "best"))
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  private def markDuplicateFragments(reads: Alignment*): Array[Alignment] = {
    val fragments: FragmentDataset = AlignmentDataset(
      spark.sparkContext.parallelize(reads),
      SequenceDictionary.empty,
      rgd,
      Seq.empty
    ).toFragments

    MarkDuplicates
      .markDuplicateFragments(fragments)
      .toAlignments
      .rdd
      .collect()
  }

  test("single fragment") {
    val read = createMappedRead("0", 100, 200)
    val marked = markDuplicateFragments(read)
    // Can't have duplicates with a single read, should return the read unchanged.
    assert(marked(0) == read)
  }

  test("fragments at different positions") {
    val read1 = createMappedRead("0", 42, 142)
    val read2 = createMappedRead("0", 43, 143)
    val marked = markDuplicateFragments(read1, read2)
    // Reads shouldn't be modified
    assert(marked.contains(read1) && marked.contains(read2))
  }

  test("fragments at the same position") {
    val poorReads = for (i <- 0 until 10) yield {
      createMappedRead("1", 42, 142, avgPhredScore = 20, readName = "poor%d".format(i))
    }
    val bestRead = createMappedRead("1", 42, 142, avgPhredScore = 30, readName = "best")
    val marked = markDuplicateFragments(List(bestRead) ++ poorReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("fragments at the same position with clipping") {
    val poorClippedReads = for (i <- 0 until 5) yield {
      createMappedRead(
        "1",
        44,
        142,
        numClippedBases = 2,
        avgPhredScore = 20,
        readName = "poorClipped%d".format(i)
      )
    }
    val poorUnclippedReads = for (i <- 0 until 5) yield {
      createMappedRead("1", 42, 142, avgPhredScore = 20, readName = "poorUnclipped%d".format(i))
    }
    val bestRead = createMappedRead("1", 42, 142, avgPhredScore = 30, readName = "best")
    val marked =
      markDuplicateFragments(List(bestRead) ++ poorClippedReads ++ poorUnclippedReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("fragments on reverse strand") {
    val poorReads = for (i <- 0 until 7) yield {
      createMappedRead(
        "10",
        42,
        142,
        isNegativeStrand = true,
        avgPhredScore = 20,
        readName = "poor%d".format(i)
      )
    }
    val bestRead = createMappedRead(
      "10",
      42,
      142,
      isNegativeStrand = true,
      avgPhredScore = 30,
      readName = "best"
    )
    val marked = markDuplicateFragments(List(bestRead) ++ poorReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("unmapped fragments") {
    val unmappedReads = for (i <- 0 until 10) yield createUnmappedRead()
    val marked = markDuplicateFragments(unmappedReads: _*)
    assert(marked.size == unmappedReads.size)
    // Unmapped reads should never be marked duplicates
    assert(marked.forall(p => !p.getDuplicateRead))
  }

  test("read pairs as fragments") {
    val poorPairs =
      for {
        i <- 0 until 10
        read <- createPair(
          "0",
          10,
          110,
          "0",
          110,
          210,
          avgPhredScore = 20,
          readName = "poor%d".format(i)
        )
      } yield read
    val bestPair = createPair("0", 10, 110, "0", 110, 210, avgPhredScore = 30, readName = "best")
    val marked = markDuplicateFragments(bestPair ++ poorPairs: _*)
    val (dups, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == "best"))
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("read pairs with fragments as fragments") {
    val fragments = for (i <- 0 until 10) yield {
      createMappedRead("2", 33, 133, avgPhredScore = 40, readName = "fragment%d".format(i))
    }
    // Even though the phred score is lower, pairs always score higher than fragments
    val pairs = createPair("2", 33, 133, "2", 100, 200, avgPhredScore = 20, readName = "pair")
    val marked = markDuplicateFragments(fragments ++ pairs: _*)
    val (dups, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == "pair"))
    assert(dups.size == 10 && dups.forall(p => p.getReadName.startsWith("fragment")))
  }

  test("chimeric fragments") {
    val poorPairs =
      for {
        i <- 0 until 10
        read <- createPair(
          "ref0",
          10,
          110,
          "ref1",
          110,
          210,
          avgPhredScore = 20,
          readName = "poor%d".format(i)
        )
      } yield read
    val bestPair =
      createPair("ref0", 10, 110, "ref1", 110, 210, avgPhredScore = 30, readName = "best")
    val marked = markDuplicateFragments(bestPair ++ poorPairs: _*)
    val (dups, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == "best"))
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  test("inverse pairs") {
    val firstPair = createPair("0", 100, 251, "0", 1100, 1251, "pair1")
    val secondPair = createPair("0", 1100, 1251, "0", 100, 251, "pair2")
    secondPair.head.setReadNegativeStrand(true)
    secondPair.head.setMateNegativeStrand(false)
    secondPair(1).setReadNegativeStrand(false)
    secondPair(1).setMateNegativeStrand(true)

    val marked = markDuplicateFragments(firstPair ++ secondPair: _*)
    val (dups, _) = marked.partition(_.getDuplicateRead)
    assert(dups.size == 2)
  }

  test("supplemental reads") {
    val supplementalPoorName = "supplementalPoor"
    val supplementalRead = createMappedRead(
      "ref0",
      10,
      110,
      "supplementalPoor",
      avgPhredScore = 10,
      isPrimaryAlignment = true
    )
    supplementalRead.setSupplementaryAlignment(true)
    val supplementalPoorPair = createPair(
        "ref0",
        10,
        110,
        "ref1",
        110,
        210,
        avgPhredScore = 30,
        readName = supplementalPoorName
      ) ++ Seq(supplementalRead)
    val bestName = "best"
    val bestPair =
      createPair("ref0", 10, 110, "ref1", 110, 210, avgPhredScore = 30, readName = bestName)
    val marked = markDuplicateFragments(bestPair ++ supplementalPoorPair: _*)
    val (_, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == bestName))
  }
}
