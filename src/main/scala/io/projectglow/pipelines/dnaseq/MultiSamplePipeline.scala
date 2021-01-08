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

import java.util.concurrent._

import scala.util.Try
import com.google.common.annotations.VisibleForTesting
import com.google.common.util.concurrent.{SimpleTimeLimiter, TimeLimiter, UncheckedTimeoutException}
import io.projectglow.common.logging.HlsUsageLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import io.projectglow.pipelines.sql.HLSConf
import io.projectglow.pipelines.{NotebookParam, Pipeline, PipelineBase, PipelineStage, SimplePipeline}

/**
 * Converts a single sample pipeline into a multi-sample pipeline.
 * Does so by first running a stage that reads the manifest file
 * and determines the samples to process.
 * Subsequently it instantiates a pipeline per sample and executes it
 */
// scalastyle:off println
trait MultiSamplePipeline
    extends PipelineBase[PipelineStage]
    with HlsUsageLogging
    with HasManifest
    with HasPerSampleTimeout {

  private var _params: Map[String, Any] = _

  override def init(params: Map[String, Any]): Unit = {
    this._params = params.filterNot {
      case (k, _) =>
        pipelineVisibleParams.map(_.name).contains(k)
    }
    this.params.collect { case p if params.contains(p.name) => set(p.name, params(p.name)) }
    // check if the manifest has been set either explicitly or via params
    require(
      this.get(manifest).nonEmpty,
      s"The path to the Manifest File ${manifest.name} must be set before the pipeline can be run"
    )
  }

  private def pipelineVisibleParams: Seq[NotebookParam] = {
    Seq(manifest, perSampleTimeout).map { p =>
      NotebookParam(
        name = p.name,
        description = p.doc,
        defaultValue = getDefault(p),
        dataType = NotebookParam.dataType(p),
        required = true
      )
    }
  }

  override def getVisibleParams: Seq[NotebookParam] = {
    // add an additional param for the manifest
    // apart from surfacing other visible params of the pipeline
    val stageParams = new SimplePipeline(this.name, stages).getVisibleParams

    // preserve the lexical sort order
    (pipelineVisibleParams ++ stageParams).sortBy(_.name)
  }

  /**
   * For each sample, returns a short description of the sample and a pipeline to run.
   */
  def prepareDelegates(session: SparkSession): Seq[(String, Pipeline[PipelineStage])]

  private def callable[T](f: => T): Callable[T] = new Callable[T] {
    override def call(): T = {
      f
    }
  }

  private def recordFailedSample(sampleDesc: String, ex: Throwable): Unit = {
    val msg =
      s"Failed processing sample $sampleDesc: ${MultiSamplePipeline.messageFromException(ex)}"
    System.err.println(msg) // print to console so message shows up in the notebook
    logger.error(msg, ex)
  }

  /**
   * Run a single sample through the pipeline. This method takes care of initialization and time
   * limiting.
   */
  @VisibleForTesting
  private[pipelines] def executeSample(
      sampleDesc: String,
      session: SparkSession,
      executor: ExecutorService,
      timeLimiter: TimeLimiter,
      pipeline: Pipeline[_]): Future[Unit] = {
    executor.submit(callable {
      try {
        timeLimiter.callWithTimeout(
          callable {
            System.gc() // Nudge Spark to cleanup unneeded files and metadata
            pipeline.init(_params)
            pipeline.execute(session)
          },
          getPerSampleTimeout.toMillis,
          TimeUnit.MILLISECONDS
        )
        System.err.println(s"Successfully processed sample $sampleDesc")
      } catch {
        case timeout @ (_: TimeoutException | _: UncheckedTimeoutException) =>
          session
            .sparkContext
            .cancelJobGroup(
              session.sparkContext.getLocalProperty(MultiSamplePipeline.SPARK_JOB_GROUP_ID_KEY)
            )
          recordFailedSample(sampleDesc, timeout)
          throw timeout
        case ex: Throwable =>
          recordFailedSample(sampleDesc, ex)
          throw ex
      }
    })
  }

  override def execute(session: SparkSession): Unit = {
    // load the manifest
    val delegates = prepareDelegates(session)
    val numThreads = SQLConf.get.getConf(HLSConf.PIPELINE_NUM_CONCURRENT_SAMPLES)
    val executor = Executors.newFixedThreadPool(numThreads)
    val timeLimiterExecutor =
      Executors.newCachedThreadPool()
    val timeLimiter = SimpleTimeLimiter.create(timeLimiterExecutor)

    val futures: List[(String, Future[Unit])] = delegates.map {
      case (sampleDesc, delegate) =>
        (sampleDesc, executeSample(sampleDesc, session, executor, timeLimiter, delegate))
    }.toList

    // wait until all tasks are done
    val results = futures.map {
      case (sampleDesc, future) => (sampleDesc, Try(future.get()))
    }
    val successfulSamples = results.filter(_._2.isSuccess).map(_._1)
    System
      .err
      .println(
        s"Successfully processed ${successfulSamples.size} / ${results.size} samples"
      )
    executor.shutdown()

    if (successfulSamples.size < results.size) {
      throw new RuntimeException(s"Failed processing samples. Please check logs for details.")
    }
  }
}

object MultiSamplePipeline {
  // Package private in Spark
  val SPARK_JOB_GROUP_ID_KEY = "spark.jobGroup.id"

  def messageFromException(ex: Throwable): String = ex match {
    case e: ExecutionException =>
      messageFromException(e.getCause)
    case _: UncheckedTimeoutException => "Timeout"
    case _: TimeoutException => "Timeout"
    case e => e.toString
  }
}
