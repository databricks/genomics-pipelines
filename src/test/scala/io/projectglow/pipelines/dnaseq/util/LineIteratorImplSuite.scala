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

package io.projectglow.pipelines.dnaseq.util

import io.projectglow.pipelines.PipelineBaseTest

class LineIteratorImplSuite extends PipelineBaseTest {
  test("Mix order of fns") {
    val baseIter = Iterator("a", "b")
    val lineIter = new LineIteratorImpl(baseIter)

    assert(lineIter.hasNext)
    assert(lineIter.peek() == "a")
    assert(lineIter.next() == "a")

    assert(lineIter.next() == "b")

    assert(!lineIter.hasNext)
    assert(lineIter.peek() == null)
    assertThrows[NoSuchElementException](lineIter.next())
  }

  test("Empty iterator") {
    val baseIter = Iterator.empty
    val lineIter = new LineIteratorImpl(baseIter)

    assert(!lineIter.hasNext)
    assertThrows[NoSuchElementException](lineIter.next())
    assert(lineIter.peek() == null)
  }
}
