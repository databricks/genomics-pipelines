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

package io.projectglow.pipelines

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

class PipelineStageSuite extends PipelineBaseTest {

  test(s"unpersists all rdds and dataframes") {
    // Keep a reference to all the cached things so that they're not automatically reclaimed
    var c1: AnyRef = null
    var c2: AnyRef = null
    var c3: AnyRef = null
    var c4: AnyRef = null
    val stage = new PipelineStage {
      override def outputExists(session: SparkSession): Boolean = false

      /**
       * Execute this stage using the supplied session.
       *
       * @param session
       */
      override def execute(session: SparkSession): Unit = {
        c1 = session.sparkContext.parallelize(1 to 10).cache()
        c2 = session.sparkContext.parallelize(1 to 10).persist(StorageLevel.DISK_ONLY)
        c3 = session.range(10).cache()
        c4 = session.range(11).persist(StorageLevel.DISK_ONLY)
      }
    }

    stage.execute(spark)

    assert(spark.sparkContext.getPersistentRDDs.nonEmpty)
    assert(!spark.sharedState.cacheManager.isEmpty)

    stage.cleanup(spark)

    // Unpersisting is async, so wrap in `eventually`
    eventually {
      assert(spark.sparkContext.getPersistentRDDs.isEmpty)
      assert(spark.sharedState.cacheManager.isEmpty)
    }
  }
}
