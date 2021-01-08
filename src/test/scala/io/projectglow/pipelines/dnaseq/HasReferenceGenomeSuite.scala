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

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import io.projectglow.pipelines.{PipelineStage, SimplePipeline}
import io.projectglow.pipelines.PipelineBaseTest

class HasReferenceGenomeSuite extends PipelineBaseTest {
  class TestStage extends SingleSampleStage with HasReferenceGenome {
    override def execute(session: SparkSession): Unit = ()

    override def outputExists(session: SparkSession): Boolean = false
  }

  test("configure with fasta file") {
    val stage = new TestStage()
    stage.setReferenceGenomeFastaPath(referenceGenomeFastaPath)
    stage.setHome("/dir")

    assert(stage.getReferenceGenomeFastaPath == referenceGenomeFastaPath)
    assert(stage.getReferenceGenomeName == referenceGenomeName)
    intercept[NoSuchElementException] {
      stage.getPrebuiltReferenceFastaPath
    }
  }

  test("configure with name and path") {
    val stage = new TestStage()
    stage.setHome("/dir")
    val path = Paths.get(referenceGenomeFastaPath).getParent.getParent.toString
    stage.set(stage.refGenomeName, referenceGenomeName)
    stage.set(stage.refGenomePath, path)
    assert(stage.getReferenceGenomeName == referenceGenomeName)
    assert(stage.getReferenceGenomeFastaPath == referenceGenomeFastaPath)
    assert(stage.getPrebuiltReferenceFastaPath == referenceGenomeFastaPath)
  }

  test("configure with id") {
    val stage = new TestStage()
    stage.setReferenceGenomeId("grch37")
    stage.setHome("/dir")
    assert(stage.getReferenceGenomeName == "human_g1k_v37")
    assert(stage.getReferenceGenomeFastaPath == "/dir/dbgenomics/grch37/data/human_g1k_v37.fa")
    assert(stage.getPrebuiltReferenceGenomePath == "/dir/dbgenomics/grch37")
  }
}
