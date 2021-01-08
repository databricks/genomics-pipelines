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

package io.projectglow.pipelines.dnaseq.alignment

import org.apache.spark.sql.functions._
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.{AlignmentDataset, DatasetBoundAlignmentDataset}
import org.bdgenomics.adam.sql.Alignment

object ConversionUtils {
  def fragmentsToReads(fragments: FragmentDataset): AlignmentDataset = {
    val df = fragments.dataset.select(explode(col("alignments")).as("rec")).select("rec.*")
    import df.sparkSession.implicits._
    DatasetBoundAlignmentDataset(
      df.as[Alignment],
      fragments.sequences,
      fragments.readGroups,
      fragments.processingSteps
    )
  }
}
