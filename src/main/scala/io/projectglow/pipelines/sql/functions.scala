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

package io.projectglow.pipelines.sql

import com.esotericsoftware.kryo.KryoSerializable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  Generator,
  GenericInternalRow,
  UnaryExpression
}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Bins the given half-open interval [start, end) using the specified binSize
 * and outputs the bins that overlap with this interval
 *
 * @param startCol the start column expression
 * @param endCol the end column expression
 * @param binSize
 */
case class BinIdGenerator(startCol: Expression, endCol: Expression, binSize: Int)
    extends Generator
    with CodegenFallback {

  override def children: Seq[Expression] = Seq(startCol, endCol)

  override def elementSchema: StructType = {
    new StructType()
      .add("binId", IntegerType, nullable = false)
      .add("isLastBin", BooleanType, nullable = false)
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val startValue = startCol.eval(input)
    val endValue = endCol.eval(input)
    if (startValue == null || endValue == null) {
      Nil
    } else {
      val start = startValue.asInstanceOf[Number].longValue()
      val end = endValue.asInstanceOf[Number].longValue()
      val sbin = Math.toIntExact(start / binSize)
      val ebin = Math.toIntExact((end - 1) / binSize)

      new Iterator[InternalRow]() {

        private var current = sbin
        private val row = new GenericInternalRow(2)

        override def hasNext: Boolean = {
          current <= ebin
        }

        override def next(): InternalRow = {
          row.setInt(0, current)
          row.setBoolean(1, current == ebin)
          current += 1
          row
        }
      }
    }
  }
}

case class StringTransformExpression(override val child: Expression, transformer: Char => Char)
    extends UnaryExpression
    with CodegenFallback {

  override def dataType: DataType = StringType

  override protected def nullSafeEval(input: Any): Any = {
    val s = input.asInstanceOf[UTF8String].toString
    val length = s.length
    val sb = new StringBuilder
    var i = 0
    while (i < length) {
      val score = s.charAt(i)
      val transformed = transformer(score)
      sb.append(transformed)
      i += 1
    }
    UTF8String.fromString(sb.toString())
  }
}
