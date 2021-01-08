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

import org.bdgenomics.adam.sql.Fragment

import io.projectglow.common.logging.HlsUsageLogging

/**
 * Transforms an iterator of raw reads into an iterator of aligned fragments.
 *
 * Unpaired reads will be discarded before alignment.
 *
 * This class ends up being much more performant than Iterator.grouped for a couple reasons:
 *
 * - Iterator.grouped gives you a scala List for each group, which has weird performance
 * characteristics. In particular, size takes linear time, which screws up many java libraries.
 *
 * - Conversion between the different sequence types was expensive
 *
 * @param batchSize How many reads to align in each JNI call
 */
class AlignmentIterator(aligner: Aligner, batchSize: Int, iterator: Iterator[AlignmentInput])
    extends Iterator[Fragment]
    with HlsUsageLogging {

  var fragmentBuffer: java.util.List[Fragment] = _
  var idx = 0
  val bufferedIterator = iterator.buffered

  override def hasNext: Boolean = {
    if (fragmentBuffer == null || idx >= fragmentBuffer.size) {
      alignNextBatch()
    }
    idx < fragmentBuffer.size
  }

  override def next(): Fragment = {
    val ret = fragmentBuffer.get(idx)
    idx += 1
    ret
  }

  private def alignNextBatch(): Unit = {
    val inputs = new java.util.ArrayList[AlignmentInput](batchSize)

    // Read up to `batchSize` paired reads
    var basesRead = 0
    while (bufferedIterator.hasNext && basesRead < batchSize) {
      val firstRead = bufferedIterator.next()
      if (aligner.pairedEndMode) {
        if (firstRead.readInFragment == 0 && bufferedIterator.hasNext) {
          val secondRead = bufferedIterator.head
          if (firstRead.readName != secondRead.readName) {
            logger.warn(s"Discarding ${firstRead.readName} because there is only one read")
          } else if (secondRead.readInFragment != 1) {
            bufferedIterator.next()
            logger.warn(s"Discarding improperly paired read ${firstRead.readName}")
          } else {
            bufferedIterator.next() // advance the iterator
            inputs.add(firstRead)
            inputs.add(secondRead)
            basesRead += firstRead.sequence.length
            basesRead += secondRead.sequence.length
          }
        }
      } else {
        inputs.add(firstRead)
        basesRead += firstRead.sequence.length
      }
    }

    // Perform alignment and convert to fragments
    fragmentBuffer = aligner.align(inputs)
    idx = 0
  }
}
