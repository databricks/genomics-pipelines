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

import scala.collection.mutable
import scala.concurrent.duration._
import io.projectglow.common.logging.HlsUsageLogging
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.SparkSession
import io.projectglow.pipelines.dnaseq.PipelineReplayMode

/**
 * Defines a pipeline as a sequence of stages.
 * To define any specific pipeline, override and create an object that
 * defines the stages of the pipeline.
 */
abstract class Pipeline[T <: PipelineStage] extends PipelineBase[T] with HlsUsageLogging {

  private var _params: Map[String, Any] = Map.empty

  override def init(params: Map[String, Any]): Unit = {
    val recognizedParamNames = stages.flatMap(s => s.params.map(_.name)).toSet
    if (!params.keySet.subsetOf(recognizedParamNames)) {
      val unrecognizedParamsStr =
        (params.keySet -- recognizedParamNames).toSeq.sorted.mkString(", ")
      throw new IllegalArgumentException(
        s"The following parameters are unrecognized: $unrecognizedParamsStr."
      )
    }

    this._params = params
  }

  override def getVisibleParams: Seq[NotebookParam] = {
    stages
      .flatMap(_.getVisibleParams)
      // get the distinct params, some params might be shared within stages
      .groupBy(_._1.name)
      .map(_._2.head)
      .toSeq
      // order them systematically
      .sortBy(_._1.name)
      .map {
        case (param, optValue) =>
          NotebookParam(
            name = param.name,
            description = param.doc,
            defaultValue = optValue.map(_.toString),
            dataType = NotebookParam.dataType(param),
            required = optValue.isEmpty
          )
      }
  }

  override def execute(session: SparkSession): Unit = {
    withInit(stage => doExecute(stage, session))
  }

  protected def doExecute(stage: T, session: SparkSession): Unit = {
    Pipeline.setPipelineConfigs(session)
    stage.init()
    if (stage.getReplayMode == PipelineReplayMode.Skip && stage.outputExists(session)) {
      // Output already exists, so skip the stage
      // scalastyle:off println
      System.err.println(s"Skipping stage ${stage.name()} because its output already exists.")
      // scalastyle:on println
    } else {
      withTiming(stage, stage.execute(session))
    }
    stage.cleanup(session)
  }

  /**
   * Executes the thunk after resolving all params (input & output) defined for this stage.
   *
   * @param thunk
   */
  private def withInit(thunk: T => Unit): Unit = {

    // Executes the supplied thunk after performing proper initialization of stages.
    // Initialization of each stage involves supplying missing parameters declared by each
    // stage as well as populating dependencies from previous stages.

    val outputColMap = new mutable.HashMap[String, Any]()

    for (stage <- stages) {

      // make sure params that are directly input are satisfied first.
      val sortedStageParams = stage
        .params
        .sortWith({ (p1, p2) =>
          val c1 = _params.contains(p1.name)
          val c2 = _params.contains(p2.name)
          (c1 compareTo c2) < 0
        })

      for (param <- sortedStageParams) {
        val p = param.asInstanceOf[Param[Any]]
        val name = param.name
        (_params ++ outputColMap).get(name).foreach { value =>
          stage.set(p, value)
        }
      }

      thunk(stage)

      // add any output columns from this stage for propagation
      val outputCols = stage.getOutputParams
      for (outputCol <- outputCols) {
        val v = stage.get(outputCol)
        require(v.isDefined, s"Param $outputCol is not defined, output column map: $outputColMap")
        val Some(value) = v
        outputColMap += ((outputCol.name, value))
      }
    }
  }

  private[pipelines] def withTiming[R](stage: PipelineStage, block: => R): R = {
    val startTime = System.nanoTime()
    // scalastyle:off println
    System.err.println(s"Running stage '${stage.description()}'...")
    val result = block // call-by-name
    val endTime = System.nanoTime()
//    val durationStr = TimeUtils.durationString((endTime - startTime).nanos)
//    System.err.println(s"Completed stage '${stage.description()}' in $durationStr")
    // scalastyle:on println
    result
  }
}

object Pipeline {
  def setPipelineConfigs(session: SparkSession): Unit = {
    // ES-17536: Make sure that we use the range join optimization in HLS pipelines
    // TODO(hhd): Remove this once the followup (SC-25874) has been addressed
    session.conf.set("spark.databricks.optimizer.convertInnerToSemiJoins.enabled", false)
  }
}

class SimplePipeline[T <: PipelineStage](override val name: String, val stages: Seq[T])
    extends Pipeline[T]
