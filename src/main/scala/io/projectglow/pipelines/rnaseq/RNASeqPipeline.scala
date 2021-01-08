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

package io.projectglow.pipelines.rnaseq

import org.apache.spark.sql.SparkSession
import io.projectglow.Glow
import io.projectglow.pipelines.{Pipeline, PipelineStage, SimplePipeline}
import io.projectglow.pipelines.dnaseq.{Manifest, MultiSamplePipeline}

/**
 * Definition of the RNA Sequencing Pipeline.
 */
object RNASeqPipeline extends MultiSamplePipeline {

  override def stages: Seq[PipelineStage] = Seq(new Align, new Count)

  override def prepareDelegates(session: SparkSession): Seq[(String, Pipeline[PipelineStage])] = {
    val manifestMetadata = Manifest.loadSampleMetadata($(manifest))(session.sqlContext).collect()
    manifestMetadata.map { sampleMetadata =>
      val stages = Seq(new Align, new Count)
      stages.foreach(_.setSampleMetadata(sampleMetadata))
      val pipeline = new SimplePipeline(this.name, stages: Seq[PipelineStage])
      (sampleMetadata.infoTag, pipeline)
    }
  }

  override def execute(session: SparkSession): Unit = {
    Glow.register(session)
    SparkSession.setActiveSession(session)
    super.execute(session)
  }
}
