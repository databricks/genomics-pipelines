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

class LocatableRegionSuite extends PipelineBaseTest {

  test("intersects") {
    val data = List(
      new SimpleInterval("A", 51, 100),
      new SimpleInterval("A", 25, 110),
      new SimpleInterval("A", 5, 110),
      new SimpleInterval("A", 15, 110),
      new SimpleInterval("B", 10, 100),
      new SimpleInterval("B", 300, 400)
    )

    val region = LocatableRegion("A", 10, 51)
    val actual = data.map(region.intersect)
    val expected = List(
      Some(new SimpleInterval("A", 51, 51)),
      Some(new SimpleInterval("A", 25, 51)),
      Some(new SimpleInterval("A", 11, 51)),
      Some(new SimpleInterval("A", 15, 51)),
      None,
      None
    )

    assert(actual sameElements expected)
  }
}
