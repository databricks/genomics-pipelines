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

package io.projectglow.pipelines.mutseq

import java.nio.file.Files

import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.PipelineBaseTest
import io.projectglow.pipelines.tnseq.TNSeqPipeline

class MutSeqPipelineSuite extends PipelineBaseTest {
  def executePipeline(pipeline: MutSeqPipelineBase): Unit = {
    val manifestFile = s"$testDataHome/raw.reads/GIAB.NA12878.20p12.TN.manifest"
    val outputDir = Files.createTempDirectory("mutseq").toAbsolutePath.toString
    val params = Map(
      "manifest" -> manifestFile,
      "output" -> outputDir,
      "referenceGenomeFastaPath" -> referenceGenomeFastaPath,
      "exportVCF" -> true
    )
    pipeline.init(params)
    pipeline.execute(spark)

    val sess = spark
    import sess.implicits._
    val reread = spark
      .read
      .format("vcf")
      .option("includeSampleIds", "true")
      .load(s"$outputDir/mutations.vcf/NA12878.vcf.bgz")

    // No mutations expected since the tumor and normal samples are the same (no-call)
    val filtered = reread.where("genotypes[0].calls[0] != -1 or genotypes[0].calls[1] != -1")
    assert(filtered.count() == 0, filtered.show())
  }

  test("execute") {
    executePipeline(MutSeqPipeline)
  }

  test("execute legacy pipeline") {
    executePipeline(TNSeqPipeline)
  }
}
