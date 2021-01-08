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

package io.projectglow.pipelines.joint

import io.projectglow.pipelines.{NotebookParam, Pipeline, PipelineStage, SimplePipeline}
import io.projectglow.pipelines.dnaseq.HasVcfManifest
import io.projectglow.pipelines.dnaseq.HasVcfManifest
import io.projectglow.pipelines.{NotebookParam, Pipeline, PipelineStage, SimplePipeline}

/**
 * Definition of the cross-cohort joint calling Pipeline.
 */
object JointGenotypingPipeline extends Pipeline[PipelineStage] with HasVcfManifest {

  override def stages: Seq[PipelineStage] = Seq(new IngestVariants, new JointlyCallVariants)

  override def getVisibleParams: Seq[NotebookParam] = {
    val manifestParam = NotebookParam(
      name = vcfManifest.name,
      description = vcfManifest.doc,
      defaultValue = Some(""),
      dataType = NotebookParam.dataType(vcfManifest),
      required = false
    )
    val stageParams = new SimplePipeline(this.name, stages).getVisibleParams

    // preserve the lexical sort order
    (Seq(manifestParam) ++ stageParams).sortBy(_.name)
  }
}
