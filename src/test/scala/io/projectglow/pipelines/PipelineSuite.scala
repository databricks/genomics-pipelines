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

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.SparkSession
import io.projectglow.pipelines.dnaseq.PipelineReplayMode

class PipelineSuite extends PipelineBaseTest {

  test("execute stage") {

    class ExampleStageOne extends PipelineStage {

      final val base_dir = new Param[String](this, "base_dir", "Root Directory")

      def setBaseDir(value: String): this.type = set(base_dir, value)

      override def outputExists(session: SparkSession): Boolean = false

      override def execute(session: SparkSession): Unit = {
        val sqlContext = session.sqlContext
        import sqlContext.implicits._
        val df = session.sparkContext.parallelize(Seq((1, "a"), (2, "b"))).toDF("id", "name")
        df.write.mode("overwrite").csv($(base_dir))
      }

    }

    val base_dir = Files.createTempDirectory("testexecutestage")
    val file = base_dir.toFile
    val path = file.getCanonicalPath

    val stage = new ExampleStageOne().setBaseDir(path)

    assert(stage.name() contains "ExampleStageOne")
    assert(stage.description() contains "ExampleStageOne")

    stage.execute(spark)

    // read back data written

    val df = this.sqlContext.read.csv(path)
    assert(df.count() == 2)

    // cleanup

    FileUtils.deleteDirectory(file)
  }

  test("execute stage with optional params") {

    class ExampleStageTwo extends PipelineStage {

      final val param_one = new Param[String](this, "param_one", "Param 1")

      final val param_two = new Param[String](this, "param_two", "Param 2")

      def setParamOne(value: String): this.type = set(param_one, value)

      def setParamTwo(value: String): this.type = set(param_two, value)

      setDefault(param_two, "PQR")

      override def outputExists(session: SparkSession): Boolean = {
        false
      }

      override def execute(session: SparkSession): Unit = {
        val sqlContext = session.sqlContext
        import sqlContext.implicits._
        val data = Seq((1, $(param_one)), (2, $(param_two)))
        val df = session.sparkContext.parallelize(data).toDF("id", "name")
        df.createOrReplaceTempView("table")
      }
    }

    val stage = new ExampleStageTwo().setParamOne("abc")
    val pipeline = new SimplePipeline("test", Seq(stage))
    pipeline.execute(spark)

    val sql = spark.sql("SELECT COUNT(*) FROM table WHERE name='PQR'")
    assert(sql.count() == 1)
  }

  test("chain dependencies of stages") {

    trait HasOutputCol extends Params {
      final val a = new Param[String](this, "a", "a")
    }

    class StageA extends PipelineStage with HasOutputCol {

      override def init(): Unit = {
        super.init()
        set(a, "ABC")
      }

      override def outputExists(session: SparkSession): Boolean = false

      override def execute(session: SparkSession): Unit = {}

      override def getOutputParams: Seq[Param[_]] = Seq(a)

    }

    class StageB extends PipelineStage with HasOutputCol {

      override def init(): Unit = {
        super.init()
        assert(this.get(a).contains("ABC"))
      }
      override def outputExists(session: SparkSession): Boolean = false

      override def execute(session: SparkSession): Unit = {}

    }

    val pipeline = new SimplePipeline("test", Seq(new StageA, new StageB))
    pipeline.execute(spark)
  }

  test("pipeline calls cleanup") {
    var wasCleanedUp = false
    val stage = new PipelineStage {
      override def outputExists(session: SparkSession): Boolean = false

      override def execute(session: SparkSession): Unit = ()

      override def cleanup(session: SparkSession): Unit = { wasCleanedUp = true }
    }
    new SimplePipeline("test", Seq(stage)).execute(spark)
    assert(wasCleanedUp)
  }

  test("replayMode = Skip") {
    var hasExecuted = false
    val stage = new PipelineStage {
      override def getReplayMode: PipelineReplayMode = PipelineReplayMode.Skip

      override def execute(session: SparkSession): Unit = {
        hasExecuted = true
      }

      override def outputExists(session: SparkSession): Boolean = {
        true
      }
    }
    new SimplePipeline("test", Seq(stage)).execute(spark)
    assert(!hasExecuted)
  }

  test("replayMode = Overwrite") {
    var hasExecuted = false
    val stage = new PipelineStage {
      override def getReplayMode: PipelineReplayMode = PipelineReplayMode.Overwrite

      override def execute(session: SparkSession): Unit = {
        hasExecuted = true
      }

      override def outputExists(session: SparkSession): Boolean = {
        true
      }
    }
    new SimplePipeline("test", Seq(stage)).execute(spark)
    assert(hasExecuted)
  }

  test("sets pipeline configs") {
    val stage = new PipelineStage {
      override def execute(session: SparkSession): Unit = {
        assert(
          !session.conf.get("spark.databricks.optimizer.convertInnerToSemiJoins.enabled").toBoolean
        )
      }

      override def outputExists(session: SparkSession): Boolean = {
        false
      }
    }
    new SimplePipeline("test", Seq(stage)).execute(spark)
  }
}
