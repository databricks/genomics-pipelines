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

package io.projectglow.pipelines.dnaseq

import org.apache.spark.sql.SparkSession
import io.projectglow.Glow
import io.projectglow.pipelines.{Pipeline, PipelineStage, SimplePipeline}

/**
 * Definition of the DNA sequencing pipeline.
 */
class DNASeqPipeline(align: Boolean, callVariants: Boolean) extends MultiSamplePipeline {

  val validatedStageMask: Seq[Boolean] = {
    val maskToValidate = Seq(align, callVariants)
    maskToValidate match {
      case Seq(false, false) =>
        throw new IllegalArgumentException("No stages to run.")
      case _ => maskToValidate
    }
  }

  override def stages: Seq[PipelineStage] = {
    Seq(new Align, new CallVariants).zip(validatedStageMask).collect {
      case (baseStage, true) => baseStage
    }
  }

  override def prepareDelegates(session: SparkSession): Seq[(String, Pipeline[PipelineStage])] = {
    val manifestMetadata = Manifest.loadSampleMetadata($(manifest))(session.sqlContext).collect()
    manifestMetadata.map { sampleMetadata =>
      val stagesWithSampleMetadata = stages
      stagesWithSampleMetadata.foreach {
        case s: HasSampleMetadata => s.setSampleMetadata(sampleMetadata)
      }
      val pipeline = new SimplePipeline(this.name, stagesWithSampleMetadata: Seq[PipelineStage])
      (sampleMetadata.infoTag, pipeline)
    }
  }

  override def execute(session: SparkSession): Unit = {
    Glow.register(session)
    SparkSession.setActiveSession(session)
    super.execute(session)
  }
}

// Default version of the pipeline with all stages enabled
object DNASeqPipeline extends DNASeqPipeline(align = true, callVariants = true)
