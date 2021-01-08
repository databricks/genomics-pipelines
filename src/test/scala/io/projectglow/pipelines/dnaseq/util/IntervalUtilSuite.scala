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

import org.broadinstitute.hellbender.utils.SimpleInterval
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.dnaseq.models.LocatableRegion

class IntervalUtilSuite extends PipelineBaseTest {

  test("trim intervals") {
    val intervals = List(
      new SimpleInterval("1", 1, 1000),
      new SimpleInterval("1", 1001, 1005), // truncated at contig length
      new SimpleInterval("2", 1001, 2000), // correct start
      new SimpleInterval("2", 2001, 3000)
    )

    val targetedRegions = Some(
      List(
        LocatableRegion("1", 500, 1010),
        LocatableRegion("2", 1500, 1800),
        LocatableRegion("2", 2500, 3500),
        LocatableRegion("2", 3500, 4000)
      )
    )

    val expected = List(
      new SimpleInterval("1", 501, 1000),
      new SimpleInterval("1", 1001, 1005),
      new SimpleInterval("2", 1501, 1800),
      new SimpleInterval("2", 2501, 3000)
    )

    assert(intervals.flatMap(IntervalUtil.trim(_, None)) sameElements intervals)
    assert(intervals.flatMap(IntervalUtil.trim(_, targetedRegions)) sameElements expected)
  }
}
