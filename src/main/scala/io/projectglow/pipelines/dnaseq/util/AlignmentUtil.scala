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

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hls.dsl.expressions.transform
import org.bdgenomics.adam.rdd.read.{QualityScoreBin, QualityScoreBinUtils}
import org.bdgenomics.adam.sql.Alignment

object AlignmentUtil {

  /**
   * Rewrites the Quality Scores attached to each read so that the score falls into the
   * supplied bins.
   *
   * @param reads The alignment records to bin the quality scores of
   * @param bins The bins to place the quality scores in
   * @return an Alignment Record Dataset containing quality scores that have been binned
   */
  def binQualityScores(
      reads: Dataset[Alignment],
      bins: Seq[QualityScoreBin]): Dataset[Alignment] = {

    // sort the bins
    val sortedBins = bins.sortBy(_.low)

    // validate the bins are non-overlapping
    sortedBins.sliding(2).foreach {
      case Seq(first, second) =>
        val overlaps = (first.low < second.high) && (second.low < first.high)
        val adjacent = first.high == second.low
        if (overlaps || !adjacent) {
          throw new IllegalStateException(s"Quality Score Bins $first, $second overlap")
        }
      case _ => // do nothing
    }

    // quality score is a string of characters, one character per site covered by the read
    // quality score binning can be thought of as a string transformation character by character

    val transformer = (phred: Char) => {
      val matchingBin = sortedBins.find(QualityScoreBinUtils.optGetBase(_, phred).nonEmpty)
      matchingBin match {
        case Some(b) => QualityScoreBinUtils.optGetBase(b, phred).get
        case None =>
          throw new IllegalStateException(
            "Quality score (%s) fell into no bins (from bins %s).".format(phred, bins.mkString(","))
          )
      }
    }

    import reads.sparkSession.implicits._
    reads
      .withColumn("binnedQualScore", transform($"qualityScores", transformer))
      .drop("qualityScores")
      .withColumnRenamed("binnedQualScore", "qualityScores")
      .as[Alignment]
  }
}
