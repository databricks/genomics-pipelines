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

import htsjdk.samtools.util.Locatable
import io.projectglow.pipelines.dnaseq.models.LocatableRegion
import io.projectglow.pipelines.dnaseq.models.LocatableRegion
import org.broadinstitute.hellbender.utils.SimpleInterval

object IntervalUtil {

  /**
   * subsets the interval to only parts that intersect with the specified targets.
   *
   * @param interval
   * @param optTargets
   * @return
   */
  def trim(
      interval: SimpleInterval,
      optTargets: Option[List[LocatableRegion]]): List[SimpleInterval] = optTargets match {
    case None => List(interval)
    case Some(targets) => targets.flatMap(_.intersect(interval))
  }

  def pointInInterval(point: Int, interval: Locatable): Boolean = {
    interval.getStart <= point && interval.getEnd >= point
  }
}
