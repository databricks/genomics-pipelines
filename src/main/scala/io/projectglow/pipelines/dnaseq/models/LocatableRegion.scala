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

package io.projectglow.pipelines.dnaseq.models

import htsjdk.samtools.util.Locatable
import org.broadinstitute.hellbender.utils.SimpleInterval

/**
 * Zero-based version of [[SimpleInterval]].
 *
 * @param referenceName The name of the sequence (chromosome) in the reference genome
 * @param start The 0-based residue-coordinate for the start of the region
 * @param end The 0-based residue-coordinate for the first residue after the start which is not in
 *            the region -- i.e. [start, end) define a 0-based half-open interval.
 */
case class LocatableRegion(referenceName: String, start: Long, end: Long) extends Locatable {
  override def getContig: String = referenceName
  override def getStart: Int = start.toInt + 1
  override def getEnd: Int = end.toInt

  lazy val asInterval: SimpleInterval = new SimpleInterval(getContig, getStart, getEnd)

  def intersect(interval: SimpleInterval): Option[SimpleInterval] = {
    if (interval.overlaps(this)) {
      Some(interval.intersect(this))
    } else {
      None
    }
  }
}
