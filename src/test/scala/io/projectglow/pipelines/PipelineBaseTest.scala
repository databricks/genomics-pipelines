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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import io.projectglow.Glow

import scala.collection.JavaConversions._ // scalastyle:ignore
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import org.apache.commons.lang.math.NumberUtils
import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructType}
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele}
import io.projectglow.common.logging.HlsUsageLogging
import io.projectglow.pipelines.common.{HLSTestData, TestUtils}
import io.projectglow.pipelines.sql.HLSConf
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.scalatest.{FunSuite, Tag}

class PipelineBaseTest
    extends FunSuite
    with TestUtils
    with HlsUsageLogging
    with HLSTestData
    with SharedSparkSessionBase {

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  override protected def sparkConf: SparkConf = {
    super
      .sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.maxResultSize", "0")
      .set(HLSConf.ASSEMBLY_REGION_NUM_PARTITIONS.key, "4")
      .set(HLSConf.VARIANT_INGEST_NUM_PARTITIONS.key, "4")
      .set(HLSConf.JOINT_GENOTYPING_NUM_BIN_PARTITIONS.key, "4")
      .set(HLSConf.JOINT_GENOTYPING_NUM_SHUFFLE_PARTITIONS.key, "4")
      .set(HLSConf.MARK_DUPLICATES_NUM_PARTITIONS.key, "4")
      .set(HLSConf.ALIGNMENT_NUM_PARTITIONS.key, "6")
      .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator")
      .set("spark.kryoserializer.buffer.max", "2047m")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.sql.extension", "io.projectglow.sql.GlowSQLExtensions")
      .set(
        "spark.hadoop.io.compression.codecs",
        "io.projectglow.sql.util.BGZFCodec"
      )
  }

  override protected def afterEach(): Unit = {
    DebugFilesystem.clearOpenStreams()
    super.afterEach()
  }

  override protected def createSparkSession = {
    val session = super.createSparkSession
    Glow.register(session)
    SparkSession.setActiveSession(session)
    session
  }

  protected def diff(
      expected: Dataset[org.bdgenomics.adam.sql.Genotype],
      actual: Dataset[org.bdgenomics.adam.sql.Genotype]): Dataset[Row] = {
    val cols = Seq("referenceName", "start", "end", "sampleId")
    val fp = expected
      .join(actual, cols, "leftanti")
      .withColumn("mismatch_type", lit("false_positive"))
    val fn = actual
      .join(expected, cols, "leftanti")
      .withColumn("mismatch_type", lit("false_negative"))
    fp.union(fn)
  }

  protected def compareGenotypes(
      truth: Genotype,
      test: Genotype,
      hasReferenceModel: Boolean = false,
      checkAnnotations: Boolean = true) {

    try {
      assert(test.getReferenceName === truth.getReferenceName)
      assert(test.getStart === truth.getStart)
      assert(test.getEnd === truth.getEnd)
      assert(test.getVariant.getReferenceAllele === truth.getVariant.getReferenceAllele)
      assert(test.getVariant.getAlternateAllele === truth.getVariant.getAlternateAllele)
      assert(test.getSampleId === truth.getSampleId)

      assert(test.getGenotypeLikelihoods.size === truth.getGenotypeLikelihoods.size)
      if (test.getGenotypeQuality != null) {
        assert(
          test.getGenotypeQuality.toDouble ~==
          truth.getGenotypeQuality.toDouble relTol 0.2,
          (test, truth)
        )
      }

      assert(test.getAlleles.size == truth.getAlleles.size)
      if (truth.getPhased) {
        (0 until truth.getAlleles.size).foreach(i => {
          val a1 = truth.getAlleles().get(i)
          val a2 = test.getAlleles().get(i)

          // we express multiallelic sites differently, specifically we set
          // one allele to a no call
          // this is not semantically different; it is identical post-normalization
          if (!(a2 == GenotypeAllele.NO_CALL &&
            (a1 == GenotypeAllele.REF || a1 == GenotypeAllele.OTHER_ALT))) {
            assert(a1 == a2)
          }
        })
      } else {
        val truthAlleles = truth.getAlleles.groupBy(identity).mapValues(_.map(_ => 1).sum)
        val testAlleles = test.getAlleles.groupBy(identity).mapValues(_.map(_ => 1).sum)
        assert(
          truthAlleles.get(GenotypeAllele.REF) == testAlleles.get(GenotypeAllele.REF) ||
          truthAlleles.get(GenotypeAllele.REF) == testAlleles.get(GenotypeAllele.NO_CALL)
        )
        assert(truthAlleles.get(GenotypeAllele.ALT) == testAlleles.get(GenotypeAllele.ALT))
        assert(
          truthAlleles.get(GenotypeAllele.OTHER_ALT) == testAlleles.get(GenotypeAllele.OTHER_ALT) ||
          truthAlleles.get(GenotypeAllele.OTHER_ALT) == testAlleles.get(GenotypeAllele.NO_CALL)
        )
      }

      // check the annotations
      if (checkAnnotations) {
        if (truth.getVariant.getAnnotation.getReadDepth != null) {
          assert(
            truth.getVariant.getAnnotation.getReadDepth ==
            test.getVariant.getAnnotation.getReadDepth
          )
        }

        // the attributes are all numbers stored as strings
        val testAnn: MMap[String, String] = test.getVariant.getAnnotation.getAttributes.asScala ++
          test.getVariantCallingAnnotations.getAttributes.asScala
        val truthAnn: MMap[String, String] = truth.getVariant.getAnnotation.getAttributes.asScala ++
          truth.getVariantCallingAnnotations.getAttributes.asScala

        assert(truthAnn.keySet.subsetOf(testAnn.keySet))
        truthAnn.foreach {
          case (k, v) =>
            // QualByDepth is randomly generated if the raw QD=QUAL/AD > 35
            if (k == "QD" && qualByDepthTooHigh(test)) {
              // No check
            } else if (NumberUtils.isNumber(v)) {
              assert(testAnn(k).toFloat ~== v.toFloat relTol 0.2)
            } else if (v.split(",").forall(NumberUtils.isNumber)) {
              val testVals = testAnn(k).split(",").map(_.toFloat)
              val truthVals = v.split(",").map(_.toFloat)
              testVals.zip(truthVals).foreach { case (v1, v2) => assert(v1 ~== v2 relTol 0.2) }
            } else {
              assert(testAnn(k) == v)
            }
        }
      }
    } catch {
      case t: Throwable =>
        logger.warn(
          s"Genotype mismatch! " +
          s"Test:\n${prettyPrint(test.toString)}\n" +
          s"Truth:\n${prettyPrint(truth.toString)}"
        )
        throw t
    }
  }

  protected def prettyPrint(s: String): String = {
    val mapper = new ObjectMapper with ScalaObjectMapper
    val obj = mapper.readValue[Map[String, Any]](s)
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

  private def qualByDepthTooHigh(gt: Genotype): Boolean = {
    val quality = gt.getVariant.getQuality
    val readDepth = gt.getReadDepth
    val maxQd = 35.0

    Option[Double](quality).getOrElse(Double.MaxValue) / readDepth > maxQd
  }

  private def qualByDepthTooHigh(qual: Double, readDepth: Int): Boolean = {
    val maxQd = 35.0
    qual / readDepth > maxQd
  }

  private def compareIntArrays(r1: Row, r2: Row, i: Int): Unit = {
    val s1 = r1.getSeq[Int](i)
    val s2 = r2.getSeq[Int](i)
    compareDoubles(s1.map(_.toDouble), s2.map(_.toDouble), r1.schema.get(i).name)
  }

  private def compareDoubleArrays(r1: Row, r2: Row, i: Int): Unit = {
    val s1 = r1.getSeq[Double](i)
    val s2 = r2.getSeq[Double](i)
    compareDoubles(s1, s2, r1.schema.get(i).name)
  }

  private def compareDoubles(s1: Seq[Double], s2: Seq[Double], fieldName: String): Unit = {
    assert(s1.size == s2.size)
    s1.zip(s2).foreach {
      case (d1, d2) =>
        assert(d1 ~== d2 relTol 0.2, s"Mismatched $fieldName")
    }
  }

  private def compareGenotypeRows(gs1: Seq[Row], gs2: Seq[Row], schema: StructType): Unit = {
    assert(gs1.size == gs2.size)
    gs1.zip(gs2).foreach {
      case (g1, g2) =>
        var i = 0
        while (i < schema.length) {
          assert(g1.isNullAt(i) == g2.isNullAt(i))
          if (!g1.isNullAt(i)) {
            if (schema(i).dataType == ArrayType(DoubleType, true)) {
              compareDoubleArrays(g1, g2, i)
            } else if (schema.fieldNames(i) == "phredLikelihoods") {
              // Random downsampling can change the likelihoods
              compareIntArrays(g1, g2, i)
            } else {
              assert(g1.get(i) == g2.get(i), s"genotype field ${schema.fieldNames(i)}")
            }
          }
          i += 1
        }
    }
  }

  def compareVariantRows(df1: DataFrame, df2: DataFrame): Unit = {
    assert(df1.count == df2.count)
    assert(df1.schema == df2.schema)
    val schema = df1.schema
    val qualIdx = schema.fieldIndex("qual")
    val gtIdx = schema.fieldIndex("genotypes")
    val gtSchema = schema
      .find(_.name == "genotypes")
      .get
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
    val readDepthIdx = gtSchema.fieldIndex("depth")
    val pairedCalls = df1.rdd.zip(df2.rdd).collect

    pairedCalls.foreach {
      case (v1, v2) =>
        var i = 0
        while (i < schema.length) {
          assert(v1.isNullAt(i) == v2.isNullAt(i))
          if (!v1.isNullAt(i)) {
            if (schema.fieldNames(i) == "INFO_QD" && qualByDepthTooHigh(
                v1.getDouble(qualIdx),
                v1.getSeq[Row](gtIdx)(0)
                  .getInt(readDepthIdx)
              )) {
              // Don't do anything
            } else if (schema(i).dataType == ArrayType(DoubleType, true)) {
              compareDoubleArrays(v1, v2, i)
            } else if (i == gtIdx) {
              compareGenotypeRows(v1.getSeq[Row](i), v2.getSeq[Row](i), gtSchema)
            } else if (schema(i).dataType == DoubleType) {
              assert(v1.getDouble(i) ~== v2.getDouble(i) relTol 0.2)
            } else if (schema.fieldNames(i) == "INFO_DP") {
              // Downsampling is random
              assert(v1.getInt(i).toDouble ~== v2.getInt(i).toDouble relTol 0.2)
            } else {
              assert(v1.get(i) == v2.get(i), s"variant field ${schema.fieldNames(i)}")
            }
          }
          i += 1
        }
    }
  }

  def compareVcfs(vcf1: String, vcf2: String): Unit = {
    val df1 = spark
      .read
      .format("vcf")
      .option("includeSampleIds", "true")
      .load(vcf1)
    val df2 = spark
      .read
      .format("vcf")
      .option("includeSampleIds", "true")
      .load(vcf2)
    compareVariantRows(df1, df2)
  }
}
