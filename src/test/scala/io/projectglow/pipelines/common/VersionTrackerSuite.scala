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

import VersionTracker.toolVersion
import io.projectglow.pipelines.PipelineBaseTest

class VersionTrackerSuite extends PipelineBaseTest {
  test("ADAM") {
    val dbr6Versions = Seq("0.30.0")
    // The suffixed version was in DBR 7.1 and 7.2, before OSS ADAM released on OSS Spark 3 post-GA
    val dbr7Versions = Seq("0.32.0-databricks1", "0.32.0")
    assert((dbr6Versions ++ dbr7Versions).contains(toolVersion("ADAM")))
  }

  test("GATK") {
    assert(toolVersion("GATK") == "4.1.4.1")
  }

  test("GATK BWA-MEM JNI") {
    assert(toolVersion("GATK BWA-MEM JNI") == "1.0.5-cb950614ce7217788780b9a8d445c64cd4d8f62e")
  }

  test("STAR") {
    assert(toolVersion("STAR") == "2.6.1a")
  }

  test("VEP") {
    assert(toolVersion("VEP") == "96")
  }
}
