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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.projectglow.common.logging._
import io.projectglow.vcf.VCFFileFormat

object VCFSaver extends HlsUsageLogging {

  def compressedVcfFileName(
      hadoopConfiguration: Configuration,
      vcfCompressionCodec: String,
      vcfOutput: String): String = {

    val factory = new CompressionCodecFactory(VCFFileFormat.hadoopConfWithBGZ(hadoopConfiguration))
    val extension = if (vcfCompressionCodec.isEmpty) {
      ""
    } else {
      Option(factory.getCodecByName(vcfCompressionCodec))
        .getOrElse(
          throw new IllegalArgumentException(
            s"Could not find compression codec for name $vcfCompressionCodec"
          )
        )
        .getDefaultExtension
    }
    vcfOutput + extension
  }

  // Deletes existing Delta and VCF output if present
  def saveToDeltaAndMaybeBigVCF(
      headerStr: String,
      df: DataFrame,
      deltaOutput: String,
      maybeCompressedVcfOutputOpt: Option[String]): Unit = {
    val session = df.sparkSession

    val path = new Path(deltaOutput)
    val fs = path.getFileSystem(session.sparkContext.hadoopConfiguration)
    fs.delete(path, true)
    DeltaHelper.save(df, deltaOutput)

    if (maybeCompressedVcfOutputOpt.isDefined) {
      fs.delete(new Path(maybeCompressedVcfOutputOpt.get), true)
      exportBigVCF(session, headerStr, deltaOutput, maybeCompressedVcfOutputOpt.get)
    }
  }

  // Deletes existing Delta and VCF output if present
  def saveToDeltaAndMaybeVCF(
      headerStr: String,
      df: DataFrame,
      exportVCF: Boolean,
      exportVCFAsSingleFile: Boolean,
      deltaOutput: String,
      deltaPartitionBy: Option[(String, Set[_ <: Any])],
      vcfOutput: String,
      vcfCompressionCodec: String): Unit = {

    val session = df.sparkSession

    val path = new Path(deltaOutput)
    val fs = path.getFileSystem(session.sparkContext.hadoopConfiguration)
    fs.delete(path, true)
    DeltaHelper.save(df, deltaOutput, deltaPartitionBy, "overwrite")

    if (exportVCF) {
      if (exportVCFAsSingleFile) {
        val maybeCompressedVcfOutput = compressedVcfFileName(
          session.sparkContext.hadoopConfiguration,
          vcfCompressionCodec,
          vcfOutput
        )
        fs.delete(new Path(maybeCompressedVcfOutput), true)
        exportBigVCF(session, headerStr, deltaOutput, maybeCompressedVcfOutput)
      } else {
        fs.delete(new Path(vcfOutput), true)
        exportShardedVCF(session, headerStr, deltaOutput, vcfOutput, vcfCompressionCodec)
      }
    }
  }

  private def exportBigVCF(
      session: SparkSession,
      headerStr: String,
      deltaOutput: String,
      maybeCompressedVcfOutput: String): Unit = {

    val df = session.read.format("delta").load(deltaOutput).sort("contigName", "start")
    df.write
      .format("bigvcf")
      .option("vcfHeader", headerStr)
      .save(maybeCompressedVcfOutput)
  }

  private def exportShardedVCF(
      session: SparkSession,
      headerStr: String,
      deltaOutput: String,
      vcfOutput: String,
      vcfCompressionCodec: String): Unit = {

    val df =
      session.read.format("delta").load(deltaOutput).sortWithinPartitions("contigName", "start")
    val vcfWriter = df
      .write
      .format("vcf")
      .option("vcfHeader", headerStr)

    val maybeCompressedVcfWriter = if (vcfCompressionCodec.nonEmpty) {
      vcfWriter.option("compression", vcfCompressionCodec)
    } else {
      vcfWriter
    }

    maybeCompressedVcfWriter.save(vcfOutput)
  }
}
