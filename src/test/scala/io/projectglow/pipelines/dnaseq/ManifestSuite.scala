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

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.mutseq.GroupedManifest

class ManifestSuite extends PipelineBaseTest {

  val readsTestDataHome = s"$testDataHome/raw.reads"

  test("load single sample, paired-ends") {
    val manifest = s"$readsTestDataHome/manifest/onesample.paired.csv"
    val samples = Manifest.loadSampleMetadata(manifest)(sqlContext)
    assert(samples.count() == 1)
    val SampleMetadata(sampleId, readGroups) = samples.take(1)(0)
    assert(sampleId == "Sample1")
    val ReadGroupMetadata(readGroupId, input, secondInput) = readGroups(0)
    assert(readGroupId == "A")
    assert(input.contains("raw.reads/a/Sample1_R1_001.fastq"))
    assert(secondInput.exists(_.contains("raw.reads/a/Sample1_R2_001.fastq")))
  }

  test("load multiple samples") {
    val manifest = s"$readsTestDataHome/manifest/multisample.paired.csv"
    val actual = Manifest
      .loadSampleMetadata(manifest)(this.sqlContext)
      .collect()
      .sortBy(_.sample_id)

    def createRecord(readGroupId: String, input: String, secondInput: String): ReadGroupMetadata = {
      ReadGroupMetadata(
        readGroupId,
        s"$readsTestDataHome/$input",
        Some(s"$readsTestDataHome/$secondInput")
      )
    }

    val expected = Array(
      SampleMetadata(
        "Sample1",
        Seq(createRecord("A", "a/Sample1_R1_001.fastq", "a/Sample1_R2_001.fastq"))
      ),
      SampleMetadata(
        "Sample2",
        Seq(createRecord("B", "a/Sample2_R1_001.fastq", "a/Sample2_R2_001.fastq"))
      ),
      SampleMetadata(
        "Sample3",
        Seq(
          createRecord("C", "a/Sample3_C_R1_*.fastq", "a/Sample3_C_R2_*.fastq"),
          createRecord("D", "a/Sample3_D_R1_*.fastq", "a/Sample3_D_R2_*.fastq")
        )
      )
    )

    assert(actual.length == expected.length)
    actual.indices.foreach { i =>
      assert(actual(i).sample_id == expected(i).sample_id)
      assert(actual(i).read_groups.toSet == expected(i).read_groups.toSet)
    }
  }

  test("resolve with no parent") {
    val manifest = ManifestBlob(spark.emptyDataFrame)
    val entryPath = "dir/sample.bam"
    assert(manifest.resolve(entryPath) == "dir/sample.bam")
  }

  test("resolve relative paths") {
    val manifest = ManifestPath(spark.emptyDataFrame, "/genomics/manifest.csv")
    val entryPath = "dir/sample.bam"
    assert(manifest.resolve(entryPath) == "/genomics/dir/sample.bam")
  }

  test("resolve absolute paths") {
    val manifest = ManifestPath(spark.emptyDataFrame, "/genomics/manifest.csv")
    val entryPath = "/dir/sample.bam"
    assert(manifest.resolve(entryPath) == "/dir/sample.bam")
  }

  test("hadoop glob characters allowed") {
    val manifest = ManifestPath(spark.emptyDataFrame, "/manifest.csv")
    val entryPath = "/{a,b}/[^a-c]?*.\\*fastq"
    assert(manifest.resolve(entryPath) == entryPath)
  }

  test("resolve across filesystems") {
    val manifest1 = ManifestPath(spark.emptyDataFrame, "s3a://bucket/path.csv")
    val entryPath1 = "dbfs:/test"
    assert(manifest1.resolve(entryPath1) == "dbfs:/test")

    val manifest2 = ManifestPath(spark.emptyDataFrame, "dbfs:/test")
    val entryPath2 = "s3a://bucket/file.bam"
    assert(manifest2.resolve(entryPath2) == "s3a://bucket/file.bam")
  }

  val vcfManifests = Seq(
    "big-files/joint/manifest.csv",
    "joint/manifest.chr21_575_split_directory.csv",
    "joint/manifest.chr21_575_split_glob.csv"
  )

  gridTest("Read VCF manifest from path")(vcfManifests) { manifest =>
    val filePaths = Manifest.loadFilePaths(s"$testDataHome/$manifest")
    assert(filePaths.head.startsWith(testDataHome))
  }

  test("Load patterns with commas") {
    val dir = Files.createTempDirectory("manifest")
    val file = dir.resolve("comma_manifest.txt")
    FileUtils.writeStringToFile(file.toFile, "/dir/{file1.vcf,file2.vcf}")
    val paths = Manifest.loadFilePaths(file.toString)
    assert(paths.head == "/dir/{file1.vcf,file2.vcf}")
  }

  test("Must have non-empty manifest") {
    val e = intercept[IllegalArgumentException](Manifest.loadFilePaths(("")))
    assert(e.getMessage.contains("Must provide a VCF manifest"))
  }

  test("Require non-empty read groups") {
    val manifest = s"$testDataHome/raw.reads/manifest/missing.readgroups.csv"
    val e = intercept[SparkException] {
      Manifest.loadSampleMetadata(manifest)(sqlContext).collect()
    }
    assert(e.getCause.isInstanceOf[IllegalArgumentException])
    assert(e.getCause.getMessage.contains("Read group ID cannot be empty"))
  }

  def compareMetadataLoader[T](
      metadataLoader: String => Dataset[T],
      manifestPath: String,
      manifestBlob: String): Unit = {
    val metadataFromPath = metadataLoader(manifestPath)
    val metadataFromBlob = metadataLoader(manifestBlob)
    assert(metadataFromBlob.schema == metadataFromPath.schema)
    assert(metadataFromBlob.collect.length == metadataFromPath.collect.length)
    metadataFromPath.collect.zip(metadataFromBlob.collect).foreach {
      case (p, b) =>
        assert(p == b)
    }
  }

  test("Read DNASeq manifest blob") {
    val parentPath = s"$testDataHome/raw.reads"
    val manifestPath = parentPath + "/manifest/multisample.paired.csv"
    val manifestBlob =
      FileUtils.readFileToString(new File(manifestPath)).replaceAll("\\.\\.", parentPath)
    compareMetadataLoader(Manifest.loadSampleMetadata(_)(sqlContext), manifestPath, manifestBlob)
  }

  test("Read MutSeq manifest blob") {
    val manifestPath = s"$readsTestDataHome/GIAB.NA12878.20p12.TN.manifest"
    val manifestBlob = FileUtils
      .readFileToString(new File(manifestPath))
      .replaceAll("GIAB.NA12878.20p12.1", s"$readsTestDataHome/GIAB.NA12878.20p12.1")
    compareMetadataLoader(
      GroupedManifest.loadSampleMetadata(_)(sqlContext),
      manifestPath,
      manifestBlob
    )
  }

  gridTest("Read VCF manifest from blob")(vcfManifests) { manifest =>
    val manifestPath = s"$testDataHome/$manifest"
    val manifestBlob = FileUtils
      .readFileToString(new File(manifestPath))
      .split("\n")
      .map { relativePath =>
        val baseDir = Paths.get(manifest).getParent.toString
        s"$testDataHome/$baseDir/$relativePath"
      }
      .mkString("\n")
    compareMetadataLoader(Manifest.loadFilePaths(_)(sqlContext), manifestPath, manifestBlob)
  }

  test("Must have real DNASeq manifest in path") {
    val e = intercept[IllegalArgumentException](
      Manifest.loadSampleMetadata(s"$readsTestDataHome/GIAB.NA12878.20p12.TN.manifest")
    )
    assert(e.getMessage.contains("Manifest read from path"))
  }

  test("Must have real DNASeq manifest in blob") {
    val e = intercept[IllegalArgumentException](Manifest.loadSampleMetadata(("fakePath.csv")))
    assert(e.getMessage.contains("Manifest read from blob"))
  }

  test("Must have real MutSeq manifest in path") {
    val e = intercept[IllegalArgumentException](
      GroupedManifest.loadSampleMetadata(s"$readsTestDataHome/manifest/multisample.paired.csv")
    )
    assert(e.getMessage.contains("Manifest read from path"))
  }

  test("Must have real MutSeq manifest in blob") {
    val e =
      intercept[IllegalArgumentException](GroupedManifest.loadSampleMetadata(("fakePath.csv")))
    assert(e.getMessage.contains("Manifest read from blob"))
  }
}
