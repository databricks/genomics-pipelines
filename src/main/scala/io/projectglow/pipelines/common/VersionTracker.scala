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

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.bdgenomics.formats.avro.ProcessingStep
import org.broadinstitute.hellbender.utils.bwa.BwaMemIndex

class VersionTracker(session: SparkSession, pipeline: String) {
  val stepBuilders: mutable.ListBuffer[ProcessingStep.Builder] =
    mutable.ListBuffer[ProcessingStep.Builder]()

  def build(firstId: Option[String]): Seq[ProcessingStep] = {
    stepBuilders.zipWithIndex.map {
      case (sb, idx) =>
        if (idx == 0) {
          sb.setPreviousId(firstId.orNull).build()
        } else {
          sb.setPreviousId(stepBuilders(idx - 1).getId).build()
        }
    }
  }

  def addStep(id: String, programName: String, commandLine: String = null): Unit = {
    val programVersion = VersionTracker.toolVersion(programName)
    val runtimeVersion = session.conf.getOption(VersionTracker.VERSION_CONF_KEY).getOrElse("")

    stepBuilders.append(
      ProcessingStep
        .newBuilder()
        .setId(id)
        .setProgramName(programName)
        .setVersion(programVersion)
        .setCommandLine(commandLine)
        .setDescription(s"Glow pipeline $pipeline $runtimeVersion")
    )
  }
}

object VersionTracker {
  val VERSION_CONF_KEY = "spark.databricks.clusterUsageTags.sparkVersion"
  val toolVersion: Map[String, String] = Map(
    "ADAM" -> "0.32.0",
    "GATK" -> "4.1.4.1",
    "GATK BWA-MEM JNI" -> Seq(
      "1.0.5",
      BwaMemIndex.getBWAVersion // Github commit for compiled version of BWA-mem
    ).mkString("-"),
    "STAR" -> "2.6.1a",
    "VEP" -> "96"
  )
}
