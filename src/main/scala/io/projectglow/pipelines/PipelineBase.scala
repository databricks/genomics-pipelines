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

import io.projectglow.pipelines.dnaseq.HasDefaultUid
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

/**
 * Abstraction for a Pipeline.
 *
 */
trait PipelineBase[T <: PipelineStage] extends HasDefaultUid with Params {

  def name: String =
    try {
      this.getClass.getSimpleName
    } catch {
      case NonFatal(ex) => this.getClass.getName
    }

  /**
   * Initializes the pipeline with any additional user supplied parameters.
   * These will override any parameters otherwise set prior to initialization.
   *
   * @param params
   */
  def init(params: Map[String, Any]): Unit

  /**
   * Obtain the user visible params of this pipeline
   * along with their default values if any
   *
   * @return
   */
  def getVisibleParams: Seq[NotebookParam]

  /**
   * Execute all stages in the pipeline.
   *
   * @param session
   */
  def execute(session: SparkSession): Unit

  def stages: Seq[T]

  override def copy(extra: ParamMap): Params = {
    throw new NotImplementedError(
      "Copy is not supported on pipelines. You can reconstruct the " +
      "pipeline from its stages"
    )
  }
}
