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

import org.apache.spark.ml.param._

/**
 * A param representation appropriate for notebook display
 *
 * @param name param name
 * @param description param documentation
 * @param defaultValue default value if any
 * @param dataType param type
 * @param required is it a required user input
 */
case class NotebookParam(
    name: String,
    description: String,
    defaultValue: Option[String],
    dataType: String,
    required: Boolean) {

  /**
   * Utility function to parse a string representation back into the expected data type
   *
   * @param value
   * @return
   */
  def coerce(value: String): Any = {
    dataType match {
      case "int" => value.toInt
      case "long" => value.toLong
      case "double" => value.toDouble
      case "boolean" => value.toBoolean
      case "String" => value
    }
  }
}

object NotebookParam {

  /**
   * Utility function to convert a param type into a String representation
   * @param param
   * @return
   */
  @SuppressWarnings(Array("This method may throw InternalError"))
  def dataType(param: Param[_]): String = {
    // only support primitive and strings for now
    val klass = param match {
      case _: IntParam => classOf[Int]
      case _: LongParam => classOf[Long]
      case _: DoubleParam => classOf[Double]
      case _: BooleanParam => classOf[Boolean]
      case _ => classOf[String]
    }

    klass.getSimpleName
  }
}
