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

package io.projectglow.pipelines.common

import io.projectglow.pipelines.PipelineBaseTest

class TimeUtilsSuite extends PipelineBaseTest {
  test("durationString") {
    import scala.concurrent.duration._
    val d1 = 1.hour
    assert(TimeUtils.durationString(d1) == "1h0m0s")

    val d2 = 13.hour + 5.minutes + 12.seconds
    assert(TimeUtils.durationString(d2) == "13h5m12s")
  }
}
