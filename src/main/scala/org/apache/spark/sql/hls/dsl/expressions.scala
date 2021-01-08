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

package org.apache.spark.sql.hls.dsl

import org.apache.spark.sql.Column
import io.projectglow.pipelines.sql.{BinIdGenerator, StringTransformExpression}

object expressions { // scalastyle:ignore

  def overlaps(x1: Column, x2: Column, y1: Column, y2: Column): Column = {
    x1 < y2 && x2 > y1
  }

  def bin(x: Column, y: Column, binSize: Int): Column =
    Column(BinIdGenerator(x.expr, y.expr, binSize))

  def transform(x: Column, transformer: Function1[Char, Char]): Column =
    Column(StringTransformExpression(x.expr, transformer))
}
