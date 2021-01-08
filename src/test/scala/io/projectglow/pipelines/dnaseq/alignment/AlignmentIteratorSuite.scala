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

import io.projectglow.pipelines.PipelineBaseTest

import scala.collection.JavaConverters._
import org.bdgenomics.adam.sql.{Fragment, Alignment => AlignmentProduct}
import org.bdgenomics.formats.avro.Alignment
import org.scalatest.mockito.MockitoSugar

class AlignmentIteratorSuite extends PipelineBaseTest with MockitoSugar {
  class DummyAligner(pairedEndModeSetting: Boolean) extends Aligner {
    override def pairedEndMode: Boolean = pairedEndModeSetting
    var callCount = 0
    override def align(inputs: java.util.List[AlignmentInput]): java.util.List[Fragment] = {
      callCount += 1
      inputs
        .asScala
        .map { input =>
          val alignRec = Alignment
            .newBuilder()
            .setSequence(new String(input.sequence))
            .setReadName(input.readName)
            .setReadInFragment(input.readInFragment)
            .build()
          Fragment(
            Option(input.readName),
            None,
            Option(input.sequence.length),
            Seq(AlignmentProduct.fromAvro(alignRec))
          )
        }
        .asJava
    }
  }

  test("no errors on empty iterator") {
    val iter = new AlignmentIterator(new DummyAligner(true), 10, Iterator.empty)
    assert(!iter.hasNext)
  }

  test("calls aligner once per chunk") {
    val aligner = new DummyAligner(true)
    val inputs =
      (0 until 10).map(n => AlignmentInput((n / 2).toString, n % 2, Array.fill(1)(1), ""))
    val outputs = new AlignmentIterator(aligner, 2, inputs.toIterator).toList
    assert(aligner.callCount == 6)
    assert(outputs.size == 10)
  }

  test("ignores unpaired reads") {
    val inputs = Seq(
      AlignmentInput("r1", 0, Array.empty, ""),
      AlignmentInput("r1", 1, Array.empty, ""),
      AlignmentInput("r2", 0, Array.empty, ""),
      AlignmentInput("r3", 0, Array.empty, ""),
      AlignmentInput("r3", 1, Array.empty, "")
    )
    val aligner = new DummyAligner(true)
    val outputs = new AlignmentIterator(aligner, 2, inputs.toIterator).toList

    assert(!outputs.exists(_.name.contains("r2")))
    assert(outputs.size == 4)
  }

  test("ignores improperly paired reads") {
    val inputs = Seq(
      AlignmentInput("r1", 0, Array.empty, ""),
      AlignmentInput("r1", 0, Array.empty, ""),
      AlignmentInput("r1", 1, Array.empty, "")
    )
    val aligner = new DummyAligner(true)
    val outputs = new AlignmentIterator(aligner, 2, inputs.toIterator).toList

    assert(outputs.isEmpty)
  }

  test("takes first two sequences if there are more than 2 for a readname") {
    val inputs = Seq(
      AlignmentInput("r1", 0, Array.empty, ""),
      AlignmentInput("r1", 1, Array.empty, ""),
      AlignmentInput("r1", 2, Array.empty, "")
    )
    val aligner = new DummyAligner(true)
    val outputs = new AlignmentIterator(aligner, 2, inputs.toIterator).toList

    assert(outputs.size == 2)
    assert(outputs.flatMap(_.alignments.map(_.readInFragment)) == Seq(Option(0), Option(1)))
  }

  test("single-end read alignment") {
    val inputs = Seq(
      AlignmentInput("r1", 0, Array.empty, ""),
      AlignmentInput("r2", 0, Array.empty, ""),
      AlignmentInput("r3", 0, Array.empty, "")
    )
    val aligner = new DummyAligner(false)
    val outputs = new AlignmentIterator(aligner, 2, inputs.toIterator).toList

    assert(outputs.size == 3)
  }
}
