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

package io.projectglow.pipelines.dnaseq

import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{length => sqlLength, _}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.adam.sql.{ProcessingStep, Alignment => AlignmentProduct}
import org.bdgenomics.formats.avro.Alignment
import picard.sam.ValidateSamFile
import io.projectglow.common.logging.HlsUsageLogging
import io.projectglow.pipelines.{PipelineBaseTest, SimplePipeline}
import io.projectglow.pipelines.{PipelineBaseTest, SimplePipeline}
import io.projectglow.pipelines.common.{DirectoryHelper, VersionTracker}
import io.projectglow.pipelines.sql.HLSConf

class AlignSuite extends PipelineBaseTest with HlsUsageLogging {

  val inputOne = s"$testDataHome/raw.reads/GIAB.NA12878.20p12.1/reads_one.fastq.gz"
  val inputTwo = s"$testDataHome/raw.reads/GIAB.NA12878.20p12.1/reads_two.fastq.gz"
  val singleEndGoldenPath = s"$testDataHome/aligned.reads/bwa_mem_single_end.bam"
  val pairedEndGoldenPath = s"$testDataHome/aligned.reads/bwa_mem_paired_end.bam"

  def createAligner: Align = {
    val outputDir = Files.createTempDirectory("output")
    new Align()
      .setSampleMetadata(
        SampleMetadata("NA12878", Seq(ReadGroupMetadata("ReadGroup", inputOne, Some(inputTwo))))
      )
      .setOutput(outputDir.toString)
      .setReferenceGenomeFastaPath(referenceGenomeFastaPath)
  }

  private def readStats(reads: AlignmentDataset): (Long, Long) = {
    val nReads = reads.dataset.count()
    val nBases = reads.dataset.agg(sum(sqlLength(col("sequence"))))

    (nReads, nBases.head().getLong(0))
  }

  test("execute") {
    val align = createAligner

    val pipeline = new SimplePipeline("test", Seq(align))
    pipeline.init(Map())
    pipeline.execute(spark)
    val aligned = spark.sparkContext.loadAlignments(align.getAlignmentOutput)
    val (nReads, nBases) = readStats(aligned)

    val sourceOfTruth = spark.sparkContext.loadAlignments(inputOne, Some(inputTwo))
    val (nGoldenReads, nGoldenBases) = readStats(sourceOfTruth)

    assert(nReads ~== nGoldenReads relTol 0.05)
    assert(nBases ~== nGoldenBases relTol 0.05)

    assertMatchesGolden(aligned)
  }

  test("exact match when using only one partition") {
    val align = createAligner
    val sess = spark.newSession() // New session so that we can change confs without side effects

    // Make the batch size match https://github.com/gatk-workflows/gatk4-data-processing/blob/master/processing-for-variant-discovery-gatk4.wdl#L57
    sess.conf.set(HLSConf.BWA_MEM_JNI_BATCH_SIZE_BASES.key, 100000000)
    sess.conf.set(HLSConf.ALIGNMENT_NUM_PARTITIONS.key, 1)
    val pipeline = new SimplePipeline("test", Seq(align))
    pipeline.init(Map())
    pipeline.execute(sess)
    val aligned = sess.sparkContext.loadAlignments(align.getAlignmentOutput)
    assertMatchesGolden(aligned, allowedMismatchRatio = 0)
  }

  test("functional equivalence") {
    val align = createAligner
      .setKnownSites(s"file:$testDataHome/known-sites/grch37/knowns.all.vcf.bgz")
      .setFunctionalEquivalence(true)

    val pipeline = new SimplePipeline("test", Seq(align))
    pipeline.init(Map())
    pipeline.execute(spark)

    val read = "HISEQ1:11:H8GV6ADXX:1:1101:10121:50392"

    val record = fetch(spark, read, align.getAlignmentOutput)
    val orig = fetch(spark, read, inputOne, Some(inputTwo))

    // expect the quality score to be recalibrated
    // TODO: Add tests for the functional equivalence steps
    assert(orig.qualityScores != record.qualityScores)
  }

  test("processing steps") {
    spark.conf.set("spark.databricks.clusterUsageTags.sparkVersion", "test-spark-version")

    val align = new Align()
      .setPairedEndMode(false)
      .setBinQualityScores(true)
      .setMarkDuplicates(true)
      .setReferenceGenomeFastaPath(referenceGenomeFastaPath)
    val steps = align.processingSteps(spark, Some("first-id"))
    val expected = Seq(
      new ProcessingStep(
        Some("BWA-MEM"),
        Some("GATK BWA-MEM JNI"),
        Some("refGenomeName=human_g1k_v37.20.21 alignPairs=false inferPairedEndStats=true"),
        Some("first-id"),
        Some("Glow pipeline DNASeq test-spark-version"),
        Some(VersionTracker.toolVersion("GATK BWA-MEM JNI"))
      ),
      new ProcessingStep(
        Some("Mark duplicates"),
        Some("ADAM"),
        None,
        Some("BWA-MEM"),
        Some("Glow pipeline DNASeq test-spark-version"),
        Some(VersionTracker.toolVersion("ADAM"))
      ),
      new ProcessingStep(
        Some("Bin quality scores"),
        Some("ADAM"),
        Some("binSpec=0,2,1;2,3,2;3,4,3;4,5,4;5,6,5;6,7,6;7,16,10;16,26,20;26,254,30"),
        Some("Mark duplicates"),
        Some("Glow pipeline DNASeq test-spark-version"),
        Some(VersionTracker.toolVersion("ADAM"))
      ),
      new ProcessingStep(
        Some("Sort"),
        Some("ADAM"),
        None,
        Some("Bin quality scores"),
        Some("Glow pipeline DNASeq test-spark-version"),
        Some(VersionTracker.toolVersion("ADAM"))
      )
    )
    assert(steps == expected.map(_.toAvro))

    spark.conf.unset("spark.databricks.clusterUsageTags.sparkVersion")
  }

  test("export bam") {
    val align = createAligner.setExportBam(true)

    align.init()
    align.execute(spark)

    // TODO(hhd): Should we have a platform value?
    val validateArgs = Array(s"I=${align.getAlignmentOutputBam}", "IGNORE=MISSING_PLATFORM_VALUE")
    assert(new ValidateSamFile().instanceMain(validateArgs) == 0)
  }

  test("replay mode") {
    val dir = Files.createTempDirectory("replay")
    val stage = new Align()
      .setSampleMetadata(SampleMetadata("sample", Seq.empty))
      .setOutput(dir.toString)
    stage.init()
    assert(!stage.outputExists(spark))
    Files.createDirectories(Paths.get(stage.getAlignmentOutput))
    Files.createFile(
      Paths
        .get(stage.getAlignmentOutput)
        .resolve(DirectoryHelper.DIRECTORY_COMMIT_SUCCESS_FILE)
    )
    assert(stage.outputExists(spark))
  }

  test("Default export settings") {
    val align = createAligner

    assert(!align.getExportBam)
    assert(align.getExportBamAsSingleFile)
    assert(align.getSortOnSave)
  }

  Seq(true, false).foreach { exportBamAsSingleFile =>
    test(s"Export BAM as single file: $exportBamAsSingleFile") {
      val align = createAligner.setExportBam(true).setExportBamAsSingleFile(exportBamAsSingleFile)

      align.init()
      align.execute(spark)

      if (exportBamAsSingleFile) {
        assert(Files.isRegularFile(Paths.get(align.getAlignmentOutputBam)))
      } else {
        val filesWritten = Files
          .list(Paths.get(align.getAlignmentOutputBam))
          .collect(Collectors.toList[Path])
          .asScala
          .map(_.toString)
        assert(filesWritten.exists(s => s.endsWith(".bam")))
      }

      val aligned = sparkContext.loadBam(align.getAlignmentOutputBam)
      assertMatchesGolden(aligned)
    }
  }

  test("Sort on save") {
    val align = createAligner.setExportBam(true).setSortOnSave(true)

    align.init()
    align.execute(spark)

    val aligned = sparkContext.loadBam(align.getAlignmentOutputBam)

    import aligned.dataset.sqlContext.implicits._

    val sortedAligned = aligned
      .dataset
      .orderBy(col("referenceName").asc_nulls_last, col("start").asc_nulls_last)
      .as[org.bdgenomics.adam.sql.Alignment]

    var isSorted = true
    aligned.dataset.collect.zip(sortedAligned.collect).foreach {
      case (ar1, ar2) =>
        if (ar1.toAvro != ar2.toAvro) {
          isSorted = false
        }
    }
    assert(isSorted)
    assertMatchesGolden(aligned)
  }

  test("Do not sort on save outputs BAM grouped by queryname") {
    val align = createAligner.setExportBam(true).setSortOnSave(false)

    align.init()
    align.execute(spark)

    val aligned = sparkContext.loadBam(align.getAlignmentOutputBam)
    val alignedList = aligned.dataset.collect

    val querynameList = alignedList.map(_.readName)
    var currQueryname: Option[String] = None
    var numQuerynameGroups = 0
    querynameList.foreach { qn =>
      if (qn != currQueryname) {
        numQuerynameGroups += 1
        currQueryname = qn
      }
    }
    assert(numQuerynameGroups == querynameList.distinct.length)
    assertMatchesGolden(aligned)
  }

  test("Run single-end read alignment") {
    val align = createAligner
      .setSampleMetadata(SampleMetadata("NA12878", Seq(ReadGroupMetadata("ReadGroup", inputOne))))
      .setPairedEndMode(false)

    val pipeline = new SimplePipeline("test", Seq(align))
    pipeline.init(Map())
    pipeline.execute(spark)
    val aligned = spark.sparkContext.loadAlignments(align.getAlignmentOutput)

    assertMatchesGolden(aligned, pairedEndMode = false)
  }

  private def assertMatchesGolden(
      test: AlignmentDataset,
      pairedEndMode: Boolean = true,
      allowedMismatchRatio: Double = 0.05): Unit = {
    val goldenPath = if (pairedEndMode) {
      pairedEndGoldenPath
    } else {
      singleEndGoldenPath
    }
    val golden = spark.sparkContext.loadAlignments(goldenPath)
    assert(test.readGroups == golden.readGroups)

    val Seq(procTest, procGolden) = Seq(test, golden).map { ardd =>
      ardd.rdd.filter(_.getReadMapped).map { rec =>
        // XA order can vary
        val xaPrefix = "XA:Z:"
        val (unsortedXaAttributes, nonXaAttributes) =
          rec.getAttributes.split("\\t").partition(_.startsWith(xaPrefix))
        val sortedXaAttributes =
          unsortedXaAttributes.map { xaAttribute =>
            // Remove the XA prefix before sorting, and re-prepend after sorting
            xaPrefix + xaAttribute.substring(xaPrefix.length).split(";").sorted.mkString(";")
          }
        val newAttributes = (nonXaAttributes ++ sortedXaAttributes).mkString("\t")

        // If a mapped read's mate is unmapped, BWA copies its alignment start and reference name
        // to the unmapped mate's corresponding fields
        val (mateStart, mateRefName) = if (rec.getMateMapped) {
          (rec.getMateAlignmentStart, rec.getMateReferenceName)
        } else {
          (null, null)
        }

        Alignment
          .newBuilder(rec)
          .setAttributes(newAttributes)
          .setInsertSize(null) // We change the insert size to match ADAM
          .setMateAlignmentStart(mateStart)
          .setMateReferenceName(mateRefName)
          .build()
      }
    }

    // Sanity check
    val allowedMismatches = procGolden.count() * allowedMismatchRatio

    val maxMismatches = Math.max(
      procGolden.subtract(procTest).count(),
      procTest.subtract(procGolden).count()
    )
    logger.info(s"Num mismatches: $maxMismatches")
    if (maxMismatches > allowedMismatches) {
      logger.info(s"Golden rows: ${procGolden.count()} Test rows: ${procTest.count()}")
      val mismatches = procGolden
        .subtract(procTest)
        .map(("GOLD", _))
        .union(procTest.subtract(procGolden).map(("TEST", _)))
        .sortBy(r => (r._2.getReadName, r._2.getReadInFragment))
        .map(_.toString)

      fail(
        s"too many mismatches! ($maxMismatches vs $allowedMismatches) \n" +
        mismatches.take(10).mkString("\n")
      )
    }
  }

  private def fetch(
      spark: SparkSession,
      read: String,
      pathOne: String,
      pathTwo: Option[String] = None): AlignmentProduct = {
    import spark.implicits._
    val results = spark.sparkContext.loadAlignments(pathOne, pathTwo)
    results
      .dataset
      .orderBy($"readName", $"start")
      .filter(_.readName contains read)
      .first()
  }

  test("checkBqsrOptions (no custom reference") {
    val align = new Align()
    align.setFunctionalEquivalence(true)
    align.checkBqsrParams() // no error
  }

  test("checkBqsrOptions (custom reference)") {
    val align = new Align() {
      override def isCustomReferenceGenome: Boolean = true
    }
    align.checkBqsrParams() // no error

    align.setFunctionalEquivalence(true)
    intercept[IllegalArgumentException] {
      align.checkBqsrParams()
    }

    align.setFunctionalEquivalence(false)
    align.set(align.recalibrateBaseQualities, true)
    intercept[IllegalArgumentException] {
      align.checkBqsrParams()
    }

    align.setKnownSites("known-sites.vcf")
    align.checkBqsrParams() // no error
  }

  test("numAlignmentPartitions") {
    val align = new Align()
    // 3 * scan partitions is less than configured min
    assert(align.numAlignmentPartitions(None, 10, 3) == 10)
    // 3 * scan partitions is greater than configured min
    assert(align.numAlignmentPartitions(None, 10, 4) == 12)
    // use exact num partitions if provided
    assert(align.numAlignmentPartitions(Some(42), 10, 3) == 42)
  }
}
