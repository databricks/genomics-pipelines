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

import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.math.min
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{Allele, GenotypeBuilder, VariantContext, VariantContextBuilder}
import htsjdk.variant.vcf.VCFHeader
import io.projectglow.vcf._
import org.broadinstitute.hellbender.engine.ReferenceFileSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.sql.HLSConf

class RegenotypedVariantRowIteratorSuite extends PipelineBaseTest {

  lazy val defaultHeaderLines = VCFMetadataLoader
    .readVcfHeader(
      spark.sparkContext.hadoopConfiguration,
      s"$testDataHome/joint/HG00096.chr20_17960111.g.vcf"
    )
    .getMetaDataInInputOrder
    .asScala
    .toSet
  lazy val defaultHeader = new VCFHeader(defaultHeaderLines.asJava)
  lazy val defaultSchema = VCFSchemaInferrer.inferSchema(true, true, defaultHeader)
  lazy val defaultRowConverter = new VariantContextToInternalRowConverter(
    defaultHeader,
    defaultSchema,
    ValidationStringency.SILENT
  )
  lazy val vcConverter = new InternalRowToVariantContextConverter(
    defaultSchema,
    defaultHeaderLines,
    ValidationStringency.SILENT
  )
  lazy val annotatedRowSchema = StructType(
    defaultSchema ++
    Seq(
      StructField("binId", IntegerType),
      StructField("isDuplicate", BooleanType),
      StructField("sampleId", StringType)
    )
  )
  lazy val annotatedRowConverter = new VariantContextToInternalRowConverter(
    defaultHeader,
    annotatedRowSchema,
    ValidationStringency.SILENT
  )
  lazy val annotatedVcConverter = new InternalRowToVariantContextConverter(
    annotatedRowSchema,
    defaultHeaderLines,
    ValidationStringency.SILENT
  )

  def makeRefBlock(sampleId: String): VariantContext = {
    val refAllele = Allele.create("A", true)
    val vcb = new VariantContextBuilder(
      "Unknown",
      "chr20",
      1,
      10,
      Seq(refAllele, Allele.NON_REF_ALLELE).asJava
    )
    val gt = new GenotypeBuilder(sampleId, Seq(refAllele, refAllele).asJava)
      .DP(38)
      .GQ(10)
      .PL(Array(0, 10, 982))
      .make
    vcb.genotypes(gt).make
  }

  def makeSpanDel(sampleId: String): VariantContext = {
    val refAllele = Allele.create("AAAAAAAAAA", true)
    val altAllele = Allele.create("A", false)
    val vcb = new VariantContextBuilder(
      "Unknown",
      "chr20",
      1,
      10,
      Seq(refAllele, altAllele, Allele.NON_REF_ALLELE).asJava
    )
    val gt = new GenotypeBuilder(sampleId, Seq(refAllele, altAllele).asJava)
      .DP(21)
      .AD(Array(12, 9, 0))
      .GQ(99)
      .PL(Array(325, 0, 503, 361, 530, 892))
      .make
    vcb.genotypes(gt).make
  }

  def makeVariant(
      sampleId: String,
      contigName: String = "chr20",
      start: Long = 6,
      referenceAlleleStr: String = "A",
      alternateAlleleStr: String = "C"): VariantContext = {
    val refAllele = Allele.create(referenceAlleleStr, true)
    val altAllele = Allele.create(alternateAlleleStr, false)
    val vcb = new VariantContextBuilder(
      "Unknown",
      contigName,
      start,
      start + refAllele.length - 1,
      Seq(refAllele, altAllele, Allele.NON_REF_ALLELE).asJava
    )
    val gt = new GenotypeBuilder(sampleId, Seq(refAllele, altAllele).asJava)
      .DP(21)
      .AD(Array(12, 9, 0))
      .GQ(99)
      .PL(Array(325, 0, 503, 361, 530, 892))
      .make
    vcb.genotypes(gt).make
  }

  def makeSplitBiallelicSnps(sampleId: String): Seq[VariantContext] = {
    val vcb = new VariantContextBuilder().source("Unknown").chr("chr20").start(6).stop(6)
    val gt = new GenotypeBuilder(sampleId, Seq(Allele.NO_CALL, Allele.NO_CALL).asJava)
      .DP(21)
      .AD(Array(12, 9, 0))
      .GQ(99)
      .PL(Array(203, 65, 60, 235, 0, 39))
      .make

    val refAllele = Allele.create("A", true)
    val altAlleleOne = Allele.create("C", false)
    val altAlleleTwo = Allele.create("T", false)
    val vcOne =
      vcb.alleles(Seq(refAllele, altAlleleOne, Allele.NON_REF_ALLELE).asJava).genotypes(gt).make
    val vcTwo =
      vcb.alleles(Seq(refAllele, altAlleleTwo, Allele.NON_REF_ALLELE).asJava).genotypes(gt).make

    Seq(vcOne, vcTwo)
  }

  def makeAnnotatedRow(vc: VariantContext, binId: Int, isDuplicate: Boolean): InternalRow = {
    val sampleId = UTF8String.fromString(vc.getGenotypes.get(0).getSampleName)
    val row = annotatedRowConverter.convertRow(vc, false)
    row.update(annotatedRowSchema.fieldIndex("binId"), binId)
    row.update(annotatedRowSchema.fieldIndex("isDuplicate"), isDuplicate)
    row.update(annotatedRowSchema.fieldIndex("sampleId"), sampleId)
    row
  }

  def getRegenotypedVariantRowIterator(
      sampleSeq: Seq[String],
      annotatedRowIter: Iterator[InternalRow],
      includeNonVariantSites: Boolean = false): RegenotypedVariantRowIterator = {

    val reference = new ReferenceFileSource(
      Paths.get(grch38Chr20to21FastaPath)
    )

    val header = new VCFHeader(defaultHeaderLines.asJava, sampleSeq.asJava)
    val engine = JointlyCallVariants.setupGenotypeGvcfsEngine(header, referenceGenomeName)
    val merger = JointlyCallVariants.createMerger(header)

    new RegenotypedVariantRowIterator(
      annotatedRowIter,
      annotatedRowSchema,
      header,
      annotatedVcConverter,
      defaultRowConverter,
      engine,
      merger,
      reference
    )
  }

  test("Regenotyper won't crash if input rows are all spanning") {
    val gt = new GenotypeBuilder("sample1").make
    val vcb = new VariantContextBuilder(
      "UNKNOWN",
      "chr20",
      1,
      10,
      Seq(Allele.create("AAAAAAAAAA", true), Allele.create("TTTTTTTTTT", false)).asJava
    )
    val locusBinDup = BinnedLocusMaybeDuplicated(Locus("chr20", 5), binId = 0, isDuplicate = false)
    val regenIterator = getRegenotypedVariantRowIterator(
      Seq("sample1"),
      Iterator(makeAnnotatedRow(vcb.genotypes(gt).make, 0, false))
    )
    regenIterator.currentLocusBinDupOpt = Some(locusBinDup)
    assert(!regenIterator.hasNext())
  }

  def checkGt(vc: VariantContext, gtIdx: Int, sampleName: String, calls: Seq[Int]): Unit = {
    val gt = vc.getGenotype(gtIdx)
    assert(gt.getSampleName == sampleName)
    assert(gt.getAlleles.size == calls.length)
    gt.getAlleles.asScala.zip(calls).foreach {
      case (a, c) =>
        if (c == -1) {
          assert(a == Allele.NO_CALL)
        } else {
          assert(a == vc.getAlleles.get(c))
        }
    }
  }

  // scalastyle:off
  def checkOutput(
      row: InternalRow,
      contig: String,
      start: Long,
      end: Long,
      referenceAllele: String,
      alternateAlleles: Seq[String],
      depth: Int,
      sample1Name: String,
      sample2Name: String,
      sample1Calls: Seq[Int],
      sample2Calls: Seq[Int]): Unit = {

    val vc = vcConverter.convert(row).get

    assert(vc.getContig == contig)
    assert(vc.getStart == start)
    assert(vc.getEnd == end)
    assert(vc.getReference == Allele.create(referenceAllele, true))
    assert(vc.getAlternateAlleles == alternateAlleles.map(Allele.create(_, false)).asJava)
    assert(vc.getAttributeAsInt("DP", 0) == depth)

    assert(vc.getGenotypes.size == 2)
    checkGt(vc, 0, sample1Name, sample1Calls)
    checkGt(vc, 1, sample2Name, sample2Calls)
  }
  // scalastyle:on

  test("Output locus with spanning ref block") {
    val sample1RefBlock = makeRefBlock("sample1")
    val sample2Snp = makeVariant("sample2")

    val inputIter = Iterator(
      makeAnnotatedRow(sample1RefBlock, binId = 0, isDuplicate = false),
      makeAnnotatedRow(sample2Snp, binId = 0, isDuplicate = false)
    )
    val outputIter = getRegenotypedVariantRowIterator(Seq("sample1", "sample2"), inputIter)

    assert(outputIter.hasNext())
    checkOutput(
      outputIter.next(),
      sample2Snp.getContig,
      sample2Snp.getStart,
      sample2Snp.getEnd,
      sample2Snp.getReference.getDisplayString,
      Seq(sample2Snp.getAlternateAllele(0).getDisplayString),
      sample1RefBlock.getGenotype(0).getDP + sample2Snp.getGenotype(0).getDP,
      "sample1",
      "sample2",
      Seq(0, 0),
      Seq(0, 1)
    )
    assert(!outputIter.hasNext())
  }

  test("Single output for single SNP") {
    val sample1Snp = makeVariant("sample1")
    val sample2Snp = makeVariant("sample2", alternateAlleleStr = "G")

    val binId = 0
    val inputIter = Iterator(
      makeAnnotatedRow(sample1Snp, binId, isDuplicate = false),
      makeAnnotatedRow(sample2Snp, binId, isDuplicate = false)
    )

    val outputIter = getRegenotypedVariantRowIterator(Seq("sample1", "sample2"), inputIter)

    assert(outputIter.hasNext())
    checkOutput(
      outputIter.next(),
      sample1Snp.getContig,
      sample1Snp.getStart,
      sample1Snp.getEnd,
      sample1Snp.getReference.getDisplayString,
      Seq(
        sample1Snp.getAlternateAllele(0).getDisplayString,
        sample2Snp.getAlternateAllele(0).getDisplayString
      ),
      sample1Snp.getGenotype(0).getDP + sample2Snp.getGenotype(0).getDP,
      "sample1",
      "sample2",
      Seq(0, 1),
      Seq(0, 2)
    )
    assert(!outputIter.hasNext())
  }

  test("No output if all duplicate") {
    val sample1Snp = makeVariant("sample1")
    val sample2Snp = makeVariant("sample2", alternateAlleleStr = "G")

    val binId = 0
    val inputIter = Iterator(
      makeAnnotatedRow(sample1Snp, binId, isDuplicate = true),
      makeAnnotatedRow(sample2Snp, binId, isDuplicate = true)
    )

    val outputIter = getRegenotypedVariantRowIterator(Seq("sample1", "sample2"), inputIter)
    assert(!outputIter.hasNext())
  }

  test("Duplicate upstream spanning deletion is represented in non-dup variants") {
    val sample1SpanDel = makeSpanDel("sample1")
    val sample2SnpOne = makeVariant("sample2")
    val sample2SnpTwo =
      makeVariant("sample2", start = sample2SnpOne.getStart + 2, alternateAlleleStr = "G")

    val binId = 1
    val inputIter = Iterator(
      makeAnnotatedRow(sample1SpanDel, binId, isDuplicate = true),
      makeAnnotatedRow(sample2SnpOne, binId, isDuplicate = false),
      makeAnnotatedRow(sample2SnpTwo, binId, isDuplicate = false)
    )

    val outputIter = getRegenotypedVariantRowIterator(Seq("sample1", "sample2"), inputIter)

    Seq(sample2SnpOne, sample2SnpTwo).foreach { snp =>
      assert(outputIter.hasNext())
      checkOutput(
        outputIter.next(),
        snp.getContig,
        snp.getStart,
        snp.getEnd,
        snp.getReference.getDisplayString,
        Seq(Allele.SPAN_DEL_STRING, snp.getAlternateAllele(0).getDisplayString),
        sample1SpanDel.getGenotype(0).getDP + snp.getGenotype(0).getDP,
        "sample1",
        "sample2",
        Seq(0, 1),
        Seq(0, 2)
      )
    }

    assert(!outputIter.hasNext())
  }

  test("Single output for single mixed site") {
    val sample1SpanDel = makeSpanDel("sample1")
    val sample2Snp = makeVariant("sample2", start = sample1SpanDel.getStart)

    val binId = 0
    val inputIter = Iterator(
      makeAnnotatedRow(sample1SpanDel, binId, isDuplicate = false),
      makeAnnotatedRow(sample2Snp, binId, isDuplicate = false)
    )

    val outputIter = getRegenotypedVariantRowIterator(Seq("sample1", "sample2"), inputIter)

    assert(outputIter.hasNext())
    checkOutput(
      outputIter.next(),
      sample1SpanDel.getContig,
      sample1SpanDel.getStart,
      sample1SpanDel.getEnd,
      sample1SpanDel.getReference.getDisplayString,
      Seq(
        sample1SpanDel.getAlternateAllele(0).getDisplayString,
        sample2Snp.getAlternateAllele(0).getDisplayString +
        sample1SpanDel.getReference.getDisplayString.drop(1)
      ),
      sample1SpanDel.getGenotype(0).getDP + sample2Snp.getGenotype(0).getDP,
      "sample1",
      "sample2",
      Seq(0, 1),
      Seq(0, 2)
    )
    assert(!outputIter.hasNext())
  }

  def checkClearedState(
      inputSeq: Seq[InternalRow],
      vc1: VariantContext,
      vc2: VariantContext): Unit = {
    // Regenotyped rows' samples should be in the same order
    val outputIter =
      getRegenotypedVariantRowIterator(Seq("sample1", "sample2"), inputSeq.toIterator)

    // First regenotyped row derived from first single-sample row, but not second
    assert(outputIter.hasNext())
    checkOutput(
      outputIter.next(),
      vc1.getContig,
      vc1.getStart,
      vc1.getEnd,
      vc1.getReference.getDisplayString,
      Seq(vc1.getAlternateAllele(0).getDisplayString),
      vc1.getGenotype(0).getDP,
      "sample1",
      "sample2",
      Seq(0, 1),
      Seq(-1, -1)
    )

    // Second regenotyped row derived from second single-sample row, but not first
    assert(outputIter.hasNext())
    checkOutput(
      outputIter.next(),
      vc2.getContig,
      vc2.getStart,
      vc2.getEnd,
      vc2.getReference.getDisplayString,
      Seq(vc2.getAlternateAllele(0).getDisplayString),
      vc2.getGenotype(0).getDP,
      "sample1",
      "sample2",
      Seq(-1, -1),
      Seq(0, 1)
    )

    assert(!outputIter.hasNext())
  }

  test("New bin") {
    val sample1Snp = makeVariant("sample1")
    val sample2Snp = makeVariant("sample2")

    val binId = 0
    val variantRowSeq = Seq(
      makeAnnotatedRow(sample1Snp, binId, isDuplicate = false),
      makeAnnotatedRow(sample2Snp, binId + 1, isDuplicate = false)
    )
    checkClearedState(variantRowSeq, sample1Snp, sample2Snp)
  }

  test("Non-matching contig") {
    val sample1Snp = makeVariant("sample1")
    val sample2Snp = makeVariant("sample2", contigName = "chr21")

    val binId = 0
    val variantRowSeq = Seq(
      makeAnnotatedRow(sample1Snp, binId, isDuplicate = false),
      makeAnnotatedRow(sample2Snp, binId, isDuplicate = false)
    )
    checkClearedState(variantRowSeq, sample1Snp, sample2Snp)
  }

  test("Non-overlapping row") {
    val sample1Snp = makeVariant("sample1")
    val sample2Snp = makeVariant("sample2", start = sample1Snp.getStart + 1)

    val binId = 0
    val variantRowSeq = Seq(
      makeAnnotatedRow(sample1Snp, binId, isDuplicate = false),
      makeAnnotatedRow(sample2Snp, binId, isDuplicate = false)
    )
    checkClearedState(variantRowSeq, sample1Snp, sample2Snp)
  }

  test("Bi-allelics are stored together in state") {
    val biallelicSnps = makeSplitBiallelicSnps("sample1")

    val binId = 0
    val inputIter = Iterator(
      makeAnnotatedRow(biallelicSnps.head, binId, isDuplicate = false),
      makeAnnotatedRow(biallelicSnps(1), binId, isDuplicate = false)
    )

    val outputIter = getRegenotypedVariantRowIterator(Seq("sample1"), inputIter)

    assert(outputIter.hasNext())
    checkOutput(
      outputIter.next(),
      biallelicSnps.head.getContig,
      biallelicSnps.head.getStart,
      biallelicSnps.head.getEnd,
      biallelicSnps.head.getReference.getDisplayString,
      Seq(
        biallelicSnps.head.getAlternateAllele(0).getDisplayString,
        biallelicSnps(1).getAlternateAllele(0).getDisplayString
      ),
      biallelicSnps.head.getGenotype(0).getDP + biallelicSnps(1).getGenotype(0).getDP,
      "sample1",
      "sample1",
      Seq(1, 2),
      Seq(1, 2)
    )
    assert(!outputIter.hasNext())
  }

  test("Exhausted iterator throws NoSuchElementException") {
    val regenIterator = getRegenotypedVariantRowIterator(Seq.empty, Iterator.empty)
    assert(!regenIterator.hasNext())
    assertThrows[NoSuchElementException](regenIterator.next())
  }

  def checkSkipNonmergeableSites(numAlts: Int, maxAlts: Int): Unit = {
    assert(numAlts > maxAlts)

    val range = (1 to numAlts)
    val sampleIds = range.map(i => s"sample$i")

    val nonMergeableRowsWithoutSpanDel = range.map { i =>
      makeAnnotatedRow(
        makeVariant(s"sample$i", start = 1000, alternateAlleleStr = "T" * i),
        isDuplicate = false,
        binId = 0
      )
    }

    val nonMergeableRowsWithSpanDel = range.map { i =>
      if (i == 1) {
        // Spanning deletion
        makeAnnotatedRow(
          makeVariant(
            s"sample$i",
            start = 1999,
            referenceAlleleStr = "AAAA",
            alternateAlleleStr = "A"
          ),
          isDuplicate = false,
          binId = 0
        )
      } else {
        makeAnnotatedRow(
          makeVariant(s"sample$i", start = 2000, alternateAlleleStr = "T" * i),
          isDuplicate = false,
          binId = 0
        )
      }
    }

    val mergeableRows = range.map { i =>
      if (i <= maxAlts) {
        makeAnnotatedRow(
          makeVariant(s"sample$i", start = 3000, alternateAlleleStr = "T" * i),
          isDuplicate = false,
          binId = 0
        )
      } else {
        // Repeated from i=1, but remapped
        makeAnnotatedRow(
          makeVariant(
            s"sample$i",
            start = 3000,
            referenceAlleleStr = "AA",
            alternateAlleleStr = "TA"
          ),
          isDuplicate = false,
          binId = 0
        )
      }
    }

    val outputIter = getRegenotypedVariantRowIterator(
      sampleIds,
      nonMergeableRowsWithoutSpanDel.toIterator ++ nonMergeableRowsWithSpanDel.toIterator ++ mergeableRows.toIterator
    )

    assert(outputIter.hasNext())
    assert(vcConverter.convert(outputIter.next()).get.getStart == 1999)
    assert(outputIter.hasNext())
    assert(vcConverter.convert(outputIter.next()).get.getStart == 3000)
    assert(!outputIter.hasNext())
  }

  test("skip highly multiallelic sites that would result in OOMs") {
    checkSkipNonmergeableSites(2000, 49)
  }

  test("configure alt allele limit") {
    val defaultMax = SQLConf.get.getConf(HLSConf.JOINT_GENOTYPING_MAX_NUM_ALT_ALLELES)
    val newMax = 20
    SQLConf.get.setConf(HLSConf.JOINT_GENOTYPING_MAX_NUM_ALT_ALLELES, newMax)
    checkSkipNonmergeableSites(21, newMax)
    SQLConf.get.setConf(HLSConf.JOINT_GENOTYPING_MAX_NUM_ALT_ALLELES, defaultMax)
  }
}
