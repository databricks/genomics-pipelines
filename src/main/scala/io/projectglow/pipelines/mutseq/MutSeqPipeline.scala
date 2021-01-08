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

package io.projectglow.pipelines.mutseq

import io.projectglow.Glow
import org.apache.spark.sql.SparkSession
import io.projectglow.pipelines.{Pipeline, PipelineStage, SimplePipeline}
import io.projectglow.pipelines.dnaseq.{Align, MultiSamplePipeline}

/**
 * Definition of the Tumor-Normal Sequencing Pipeline.
 */
trait MutSeqPipelineBase extends MultiSamplePipeline {

  override def stages: Seq[PipelineStage] = Seq(new Align, new Align, new CallMutations)

  override def prepareDelegates(session: SparkSession): Seq[(String, Pipeline[PipelineStage])] = {
    val sampleMetadata = GroupedManifest
      .loadSampleMetadata($(manifest))(session.sqlContext)
      .collect()
    sampleMetadata.map { metadata =>
      val tumorSamples = metadata.samples.collect { case (label, data) if label == "tumor" => data }
      val normalSamples =
        metadata.samples.collect { case (label, data) if label == "normal" => data }
      require(tumorSamples.size == 1, "Must provide exactly one tumor sample per pair")
      require(
        Seq(0, 1).contains(normalSamples.size),
        "Must provide at most one normal sample per pair"
      )
      val normalAlign = normalSamples
        .headOption
        .map(new Align("Normal").setSampleMetadata)
      val tumorAlign = new Align("Tumor").setSampleMetadata(tumorSamples.head)
      val callMutations = new CallMutations().setGroupedSampleMetadata(metadata)
      val stages = normalAlign ++ Seq(tumorAlign, callMutations)
      (metadata.infoTag, new SimplePipeline(this.name, stages.toSeq: Seq[PipelineStage]))
    }
  }

  override def execute(session: SparkSession): Unit = {
    Glow.register(session)
    SparkSession.setActiveSession(session)
    super.execute(session)
  }
}

object MutSeqPipeline extends MutSeqPipelineBase
