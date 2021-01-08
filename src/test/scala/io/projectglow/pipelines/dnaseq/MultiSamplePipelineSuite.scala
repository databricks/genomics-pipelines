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

import java.util.concurrent.{ExecutionException, Executors, TimeoutException}

import scala.collection.concurrent.TrieMap
import com.google.common.util.concurrent.{SimpleTimeLimiter, UncheckedTimeoutException}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.SparkSession
import io.projectglow.pipelines.{NotebookParam, Pipeline, PipelineBaseTest, PipelineStage, SimplePipeline}

class MultiSamplePipelineSuite extends PipelineBaseTest {

  test("multisample pipeline") {

    // keep track of samples and stages invoked
    val counts = TrieMap[String, List[String]]()

    def updateCounts(stage: SingleSampleStage): Unit = {
      val sampleId = stage.getSampleMetadata.sample_id
      val stages = counts.getOrElse(sampleId, List.empty)
      counts.put(sampleId, stages ++ List(stage.name()))
    }

    class StageOne extends SingleSampleStage {
      final val paramOne = new Param[String](this, "paramOne", "Param 1")

      override def name(): String = "StageOne"

      def setParamOne(value: String): this.type = set(paramOne, value)

      override def outputExists(session: SparkSession): Boolean = false

      override def execute(session: SparkSession): Unit = {
        assert(this.get(paramOne).isDefined)
        updateCounts(this)
      }
    }

    class StageTwo extends SingleSampleStage {
      final val paramTwo = new Param[String](this, "paramTwo", "Param 2")

      override def name(): String = "StageTwo"

      def setParamTwo(value: String): this.type = set(paramTwo, value)

      override def outputExists(session: SparkSession): Boolean = false

      override def execute(session: SparkSession): Unit = {
        assert(this.get(paramTwo).isDefined)
        updateCounts(this)
      }
    }

    val manifest = s"$testDataHome/raw.reads/manifest/multisample.paired.csv"

    val pipeline = new MultiSamplePipeline {
      override def prepareDelegates(
          session: SparkSession): Seq[(String, Pipeline[PipelineStage])] = {
        Manifest.loadSampleMetadata($(manifest))(session.sqlContext).collect().map { sample =>
          val stages = Seq(new StageOne, new StageTwo)
          stages.foreach(_.setSampleMetadata(sample))
          (sample.infoTag, new SimplePipeline("test", stages: Seq[PipelineStage]))
        }
      }

      override def getVisibleParams: Seq[NotebookParam] = ??? // scalastyle:ignore

      override def stages: Seq[PipelineStage] = Seq(new StageTwo, new StageTwo)
    }
    val params = Map("paramOne" -> "a", "paramTwo" -> "b", "manifest" -> manifest)
    pipeline.init(params)
    pipeline.execute(spark)
    // require both stages to be executed thrice, once per sample
    assert(counts.size == 3)
    assert(counts.keySet sameElements Set("Sample1", "Sample2", "Sample3"))
    val stageNames = Seq("StageOne", "StageTwo")
    counts.values.foreach { value =>
      // exactly two stages should be executed for each sample
      assert(value == stageNames)
    }
  }

  test("executeSample enforces timeout") {
    val sess = spark
    import scala.concurrent.duration._
    import sess.implicits._

    val stage = new PipelineStage {
      override def execute(session: SparkSession): Unit = {
        // Split the sleep over many rows so that the task is cancellable
        session
          .range(3000)
          .map { _ =>
            Thread.sleep(1)
            1
          }
          .collect()
      }

      override def outputExists(session: SparkSession): Boolean = false
    }

    val innerPipeline = new SimplePipeline("pipeline", Seq(stage))

    val pipeline = new MultiSamplePipeline {
      override def prepareDelegates(
          session: SparkSession): Seq[(String, Pipeline[PipelineStage])] = {
        Seq(("sample", innerPipeline))
      }

      override def stages: Seq[PipelineStage] = Seq(stage)
    }

    pipeline.init(Map("manifest" -> "/dev/null"))

    pipeline.setPerSampleTimeout("1s")
    val es = Executors.newCachedThreadPool()
    val timeLimiter = SimpleTimeLimiter.create(es)

    // Expect a timeout exception when the job takes longer than the per sample timeout
    val ex = intercept[ExecutionException] {
      pipeline.executeSample("sample", spark, es, timeLimiter, innerPipeline).get()
    }
    assert(ex.getCause.isInstanceOf[TimeoutException])
    eventually {
      // Make sure all the jobs are canceled
      assert(spark.sparkContext.statusTracker.getActiveJobIds().isEmpty)
    }

    // Make the timeout longer and make sure the pipeline succeeds
    pipeline.setPerSampleTimeout("12h")
    val res = pipeline.executeSample("sample", spark, es, timeLimiter, innerPipeline)
    res.get()
  }

  test("messageFromException") {
    assert(MultiSamplePipeline.messageFromException(new UncheckedTimeoutException("")) == "Timeout")
    val wrappedTimeout = new ExecutionException(new UncheckedTimeoutException(""))
    assert(MultiSamplePipeline.messageFromException(wrappedTimeout) == "Timeout")
    val runtimeException = new RuntimeException("monkey")
    assert(MultiSamplePipeline.messageFromException(runtimeException) == runtimeException.toString)
    val wrappedRuntimeException = new ExecutionException(new RuntimeException("monkey"))
    assert(
      MultiSamplePipeline.messageFromException(wrappedRuntimeException) == runtimeException.toString
    )
  }
}
