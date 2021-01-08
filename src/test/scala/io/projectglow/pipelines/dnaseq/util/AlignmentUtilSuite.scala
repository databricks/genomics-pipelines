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

import io.projectglow.pipelines.PipelineBaseTest
import org.bdgenomics.adam.rdd.read.{AlignmentDataset, QualityScoreBin}
import org.bdgenomics.formats.avro.Alignment

class AlignmentUtilSuite extends PipelineBaseTest {

  test("bin quality scores") {
    val sequence = Seq(0, 10, 20, 25, 25, 22, 20, 19, 18)
      .map(i => (i + 33).toChar)
      .mkString

    val read = Alignment
      .newBuilder
      .setQualityScores(sequence)
      .setSequence("ACAGATTCG")
      .setReadName("aRead")
      .build
    val reads = AlignmentDataset
      .unaligned(sparkContext.parallelize(Seq(read)))
      .dataset

    val bins = Seq(QualityScoreBin(0, 20, 10), QualityScoreBin(20, 50, 40))

    val binnedReads = AlignmentUtil.binQualityScores(reads, bins).collect()

    def testQuals(qualString: String) {
      val newQuals = qualString.map(i => i.toInt - 33)

      assert(newQuals.size === 9)
      assert(newQuals(0) === 10)
      assert(newQuals(1) === 10)
      assert(newQuals(2) === 40)
      assert(newQuals(3) === 40)
      assert(newQuals(4) === 40)
      assert(newQuals(5) === 40)
      assert(newQuals(6) === 40)
      assert(newQuals(7) === 10)
      assert(newQuals(8) === 10)
    }

    assert(binnedReads.length == 1)
    binnedReads.foreach(read => testQuals(read.qualityScores.get))
  }
}
