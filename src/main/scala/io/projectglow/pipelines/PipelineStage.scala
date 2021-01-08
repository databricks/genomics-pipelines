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
import scala.reflect.runtime.universe.{runtimeMirror, typeOf, TermSymbol}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.SparkSession
import io.projectglow.common.logging.HlsUsageLogging
import io.projectglow.pipelines.dnaseq.{HasDefaultUid, HasReplayMode}

import scala.util.control.NonFatal

/**
 * Contract for a stage of a pipeline.
 */
trait PipelineStage extends HlsUsageLogging with HasDefaultUid with HasReplayMode {

  def init(): Unit = {}

  /**
   * Execute this stage using the supplied session.
   *
   * @param session
   */
  def execute(session: SparkSession): Unit

  def outputExists(session: SparkSession): Boolean

  /**
   * Cleanup steps to run after executing this stage. By default, uncaches all RDDs and DataFrames.
   */
  def cleanup(session: SparkSession): Unit = {
    session.sqlContext.clearCache()
    session.sparkContext.getPersistentRDDs.foreach {
      case (_, rdd) =>
        rdd.unpersist()
    }
  }

  def getOutputParams: Seq[Param[_]] = Seq.empty

  /**
   * Name of the pipeline stage
   *
   * @return
   */
  def name(): String =
    try {
      this.getClass.getSimpleName
    } catch {
      case NonFatal(ex) => this.getClass.getName
    }

  /**
   * Description of the pipeline
   *
   * @return
   */
  def description(): String = name()

  override def copy(extra: ParamMap): this.type = defaultNoArgCopy(extra)

  /**
   * Default implementation of copy with extra params.
   * It expects a no-arg constructor
   * Then it copies the embedded and extra parameters over and returns the new instance.
   */
  protected final def defaultNoArgCopy[T <: Params](extra: ParamMap): T = {
    val that = this.getClass.newInstance()
    copyValues(that, extra).asInstanceOf[T]
  }

  /**
   * Obtain the parameters of this stage that are visible to the end user.
   * Any parameter that needs to be set by the end user should be marked [UserVisible]
   *
   * @return
   */
  def getVisibleParams: Seq[(Param[Any], Option[Any])] = {

    def runtimeType[T](klass: Class[T]) = {
      val m = runtimeMirror(klass.getClassLoader)
      val classSymbol = m.staticClass(klass.getName)
      classSymbol.selfType
    }

    def findVisibleParams(klass: Class[_], acc: mutable.Set[String]): Unit = {
      val fields = runtimeType(klass)
        .members
        .collect { case s: TermSymbol => s }
        .filter(s => s.isVal)

      // extract the fields with @UserVisible annotation
      val userVisibleFields = fields.flatMap { f =>
        f.annotations
          .find(_.tpe =:= typeOf[UserVisible])
          .map { _ =>
            val fullName = f.fullName
            val i = fullName.lastIndexOf('.')
            fullName.substring(i + 1)
          }
      }.toSet

      acc.++=(userVisibleFields)
    }

    // walk through the class hierarchy of this class to identify
    // params that it contains
    val userVisibleFields = new mutable.HashSet[String]()
    val classesToWalk = new mutable.Stack[Class[_]]()
    classesToWalk.push(getClass)
    while (classesToWalk.nonEmpty) {
      val klass = classesToWalk.pop()
      findVisibleParams(klass, userVisibleFields)
      classesToWalk.pushAll(klass.getInterfaces)
    }

    // assumes the param name matches the variable name
    params.filter { param =>
      userVisibleFields.contains(param.name)
    }.map { param =>
      (param.asInstanceOf[Param[Any]], getDefault(param))
    }
  }
}
