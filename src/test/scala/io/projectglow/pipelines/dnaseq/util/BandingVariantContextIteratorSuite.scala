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

package io.projectglow.pipelines.dnaseq.util

import scala.collection.JavaConverters._
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeader
import io.projectglow.pipelines.PipelineBaseTest
import org.apache.spark.DebugFilesystem
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.HaplotypeCallerArgumentCollection

class BandingVariantContextIteratorSuite extends PipelineBaseTest {

  test("load in and band variant contexts") {
    val sc = spark.sparkContext
    val gatkCallsPath = s"$testDataHome/dnaseq/NA12878_21_10002403.bp.g.vcf"

    val vcRdd = sc.loadGenotypes(gatkCallsPath)
    val converter =
      new VariantContextConverter(vcRdd.headerLines, ValidationStringency.LENIENT, true)

    val vcs = vcRdd
      .rdd
      .filter(gt => {
        gt.getStart >= 10002393L &&
        gt.getStart < 10002398L
      })
      .collect
      .map(gt => VariantContext.buildFromGenotypes(Seq(gt)))
      .flatMap(vc => converter.convert(vc))

    assert(vcs.size === 5)

    val bandedVcs = new BandingVariantContextIterator(
      new VCFHeader(vcRdd.headerLines.toSet.asJava),
      new HaplotypeCallerArgumentCollection().GVCFGQBands.asScala.map(n => n: Int),
      vcs.iterator
    ).flatMap(vc => converter.convert(vc)).toArray

    assert(bandedVcs.size === 1)
    val gt = bandedVcs.flatMap(_.genotypes).head
    assert(gt.getStart === 10002393L)
    assert(gt.getEnd === 10002398L)
  }
}
