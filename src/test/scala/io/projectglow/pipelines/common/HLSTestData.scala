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

trait HLSTestData {

  final lazy val testDataHome = s"${System.getProperty("user.dir")}/test-data"
  final lazy val bigFilesHome = s"$testDataHome/big-files"
  final val referenceGenomeName = "human_g1k_v37.20.21"
  final lazy val referenceGenomeFastaPath = {
    s"$bigFilesHome/$referenceGenomeName/data/$referenceGenomeName.fa"
  }

  final lazy val grch38Chr20to21 = "Homo_sapiens_assembly38.20.21"
  final lazy val grch38Chr20to21FastaPath = {
    s"$bigFilesHome/$grch38Chr20to21/data/$grch38Chr20to21.fa"
  }
}
