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

import scala.collection.JavaConverters._

import htsjdk.variant.vcf.VCFHeader
import io.projectglow.vcf.{VCFMetadataLoader, VCFSchemaInferrer}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bdgenomics.adam.util.FileExtensions

object VCFReader {

  /**
   * If path points to a VCF:
   *   - Creates DataFrame with VCF reader
   *   - Parses VCF header from the VCF
   * Otherwise:
   *   - Creates DataFrame with Delta reader
   *   - Creates VCF header based on the schema, and sample IDs from the first row.
   *
   * When reading from a Delta table, there must be at least one row to build the VCF header.
   */
  def readFromDeltaOrVCF(spark: SparkSession, path: String): (VCFHeader, DataFrame) = {
    if (FileExtensions.isVcfExt(path)) {
      val vcfHeader = VCFMetadataLoader.readVcfHeader(spark.sparkContext.hadoopConfiguration, path)
      val vcfDf = spark
        .read
        .format("vcf")
        .option("includeSampleIds", true)
        .load(path)
      (vcfHeader, vcfDf)
    } else {
      readFromDelta(spark, path)
    }
  }

  private def readFromDelta(spark: SparkSession, path: String): (VCFHeader, DataFrame) = {
    // If it looks like we're trying to put a partition predicate in the path, turn it into
    // a where clause
    val lastSlash = path.stripSuffix("/").lastIndexOf('/')
    val vcfDf = path.drop(lastSlash + 1).split('=') match {
      case Array(key, value) =>
        spark.read.format("delta").load(path.take(lastSlash)).where(col(key) === lit(value))
      case _ =>
        spark.read.format("delta").load(path)
    }

    if (vcfDf.isEmpty) {
      throw new IllegalArgumentException(s"No variant data at $path")
    }

    val headerLines = VCFSchemaInferrer.headerLinesFromSchema(vcfDf.schema)
    val sampleIds = vcfDf.limit(1).select("genotypes.sampleId").head.getAs[Seq[String]](0)
    val vcfHeader = new VCFHeader(headerLines.toSet.asJava, sampleIds.asJava)
    (vcfHeader, vcfDf)
  }
}
