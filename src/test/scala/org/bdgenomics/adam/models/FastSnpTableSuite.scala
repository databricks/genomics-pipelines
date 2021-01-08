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

package org.bdgenomics.adam.models

import io.projectglow.pipelines.PipelineBaseTest
import org.bdgenomics.adam.rdd.ADAMContext._

/**
 * This test suite and associated test files are based on SnpTableSuite in ADAM.
 * https://github.com/bigdatagenomics/adam/blob/master/adam-core/src/test/scala/org/bdgenomics/adam/models/SnpTableSuite.scala
 *
 * Differences from the original SnpTableSuite are noted inline.
 */
class FastSnpTableSuite extends PipelineBaseTest {

  test("create a snp table from variants on multiple contigs") {
    val inputPath = s"$testDataHome/adam/random.vcf"
    val table = FastSnpTable.fromVcf(spark, inputPath)

    // DIFFERENCE: ADAM's SnpTable is sorted by contig index instead of contig name, so the contigs
    // are in a different order
    assert(table.indices.size == 3)
    assert(table.indices("1") == ((0, 2)))
    assert(table.indices("2") == ((5, 5)))
    assert(table.indices("13") == ((3, 4)))
    assert(table.sites.length == 6)
    assert(table.sites(0) == 14396L)
    assert(table.sites(1) == 14521L)
    assert(table.sites(2) == 63734L)
    assert(table.sites(3) == 752720L)
    assert(table.sites(4) == 752790L)
    assert(table.sites(5) == 19189L)
  }

  test("create a snp table from a larger set of variants") {
    val sess = spark
    import sess.implicits._
    val inputPath = s"$testDataHome/adam/bqsr1.vcf"
    val variants = spark.read.format("vcf").load(inputPath)
    val numVariants = variants.count()
    val table = FastSnpTable.fromVcf(spark, inputPath)
    assert(table.indices.size == 1)
    assert(table.indices("22") == ((0, numVariants - 1)))
    assert(table.sites.length == numVariants)
    val variantsByPos = variants
      .select("start")
      .as[Long]
      .collect
      .sorted
    table
      .sites
      .zip(variantsByPos)
      .foreach(p => {
        assert(p._1 == p._2)
      })
  }

  // DIFFERENCE: Compare with ADAM's SnpTable in the following two suites in addition to checking
  // exact output. Test cases are unchanged.
  test("perform lookups on multi-contig snp table") {
    val path = s"$testDataHome/adam/random.vcf"
    val table = FastSnpTable.fromVcf(spark, path)
    val adamTable = SnpTable(spark.sparkContext.loadVariants(path))

    val region1 = ReferenceRegion("1", 14390L, 14530L)
    val s1 = table.maskedSites(region1)
    assert(s1.size == 2)
    assert(s1(14396L))
    assert(s1(14521L))
    assert(s1 == adamTable.maskedSites(region1))

    val region2 = ReferenceRegion("13", 752700L, 752800L)
    val s2 = table.maskedSites(region2)
    assert(s2.size == 2)
    assert(s2(752720L))
    assert(s2(752790L))
    assert(s2 == adamTable.maskedSites(region2))
  }

  test("perform lookups on larger snp table") {
    val path = s"$testDataHome/adam/bqsr1.vcf"
    val table = FastSnpTable.fromVcf(spark, path)
    val adamTable = SnpTable(spark.sparkContext.loadVariants(path))

    val region1 = ReferenceRegion("22", 16050670L, 16050690L)
    val s1 = table.maskedSites(region1)
    assert(s1.size == 2)
    assert(s1(16050677L))
    assert(s1(16050682L))
    assert(s1 == adamTable.maskedSites(region1))

    val region2 = ReferenceRegion("22", 16050960L, 16050999L)
    val s2 = table.maskedSites(region2)
    assert(s2.size == 3)
    assert(s2(16050966L))
    assert(s2(16050983L))
    assert(s2(16050993L))
    assert(s2 == adamTable.maskedSites(region2))

    val region3 = ReferenceRegion("22", 16052230L, 16052280L)
    val s3 = table.maskedSites(region3)
    assert(s3.size == 4)
    assert(s3(16052238L))
    assert(s3(16052239L))
    assert(s3(16052249L))
    assert(s3(16052270L))
    assert(s3 == adamTable.maskedSites(region3))
  }
}
