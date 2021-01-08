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

import java.util

import scala.collection.JavaConverters._

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.variantcontext.writer.VariantContextWriter
import htsjdk.variant.vcf.VCFHeader
import org.broadinstitute.hellbender.utils.variant.writers.GVCFWriter

/**
 * GATK supports a gVCF mode where it emits a record for every site covered by the input reads, but
 * bands adjacent hom-ref sites together to save space. This class wraps the gVCF writer that
 * performs this banding and exposes the functionality as a iterator, which is better suited for
 * our DNASeq pipeline.
 *
 * For more info on VCF vs gVCF vs BP-Resolution VCF, consult:
 * https://software.broadinstitute.org/gatk/documentation/article.php?id=4017
 *
 * @param header The VCF header. Not really used, but required by GATK.
 * @param bandingSpec Spec used to determine whether a new genotype can be banded with its
 *                    adjacent sites. The number refers to the phred genotype quality. Each number
 *                    is the start point for a new band. There's an implicit 0 added to the
 *                    beginning, so for example a spec of [50] will result in a band of [0, 49)
 *                    and a band of [50, 100].
 * @param vcs The variant contexts
 */
class BandingVariantContextIterator(
    header: VCFHeader,
    bandingSpec: Seq[Int],
    vcs: Iterator[VariantContext])
    extends Iterator[VariantContext] {
  private val stateWriter = new VariantContextQueueWriter()
  private val gvcfWriter = new GVCFWriter(stateWriter, bandingSpec.map(n => n: Number).asJava, 2)
  gvcfWriter.setHeader(header)

  override def hasNext: Boolean = {
    while (stateWriter.queue.isEmpty && vcs.hasNext) {
      gvcfWriter.add(vcs.next())
    }

    // If we've added all the variant contexts, flush the writer buffer in case there's an active
    // hom-ref block
    if (!vcs.hasNext) {
      gvcfWriter.close()
    }

    !stateWriter.queue.isEmpty
  }

  override def next(): VariantContext = {
    stateWriter.queue.remove()
  }
}

/**
 * A utility class that implements the GATK's [[VariantContextWriter]] interface, but writes
 * results to an in-memory queue rather than an IO stream.
 */
private[util] class VariantContextQueueWriter extends VariantContextWriter {
  val queue: util.Queue[VariantContext] = new util.LinkedList()

  override def setHeader(vcfHeader: VCFHeader): Unit = ()

  override def writeHeader(vcfHeader: VCFHeader): Unit = ()

  override def checkError(): Boolean = false

  override def close(): Unit = ()

  override def add(variantContext: VariantContext): Unit = {
    queue.add(variantContext)
  }
}
