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

import org.apache.spark.sql.{Dataset, SQLContext}
import io.projectglow.pipelines.dnaseq
import io.projectglow.pipelines.dnaseq.{HasSampleInfo, Manifest, ManifestEntry, SampleMetadata}

/**
 * Sample Manifest Entry Definition.
 * There is exactly one manifest entry per (sample, read_group)
 *
 * @param file_path  The relative path of the sample
 * @param sample_id  The sample id
 * @param paired_end The paired end this manifest entry corresponds to (1|2|empty)
 */
case class GroupedManifestEntry(
    pair_id: String,
    file_path: String,
    sample_id: String,
    label: String,
    read_group_id: String,
    paired_end: String) {

  def toManifestEntry: ManifestEntry = {
    ManifestEntry(file_path, sample_id, read_group_id, paired_end)
  }
}

case class GroupedSampleMetadata(pair_id: String, samples: Seq[(String, SampleMetadata)])
    extends HasSampleInfo {
  override def infoTag: String = pair_id
}

object GroupedManifest {

  /**
   * Load sample metadata from a manifest.
   *
   * @param manifestBlobOrPath path to the manifest file, or a blob containing the manifest
   * @param sqlContext
   * @return
   */
  def loadSampleMetadata(manifestBlobOrPath: String)(
      implicit sqlContext: SQLContext): Dataset[GroupedSampleMetadata] = {
    // pair_id,file_path,sample_id,label,paired_end,read_group_id
    val expectedColumns = Manifest.getCaseClassFieldsAndTypes[GroupedManifestEntry].map(_._1)
    val manifest = dnaseq.Manifest.loadCsvManifest(manifestBlobOrPath, expectedColumns)(sqlContext)

    import sqlContext.implicits._
    val manifestEntryDs = manifest
      .contents
      .na
      .fill("")
      .as[GroupedManifestEntry]

    manifestEntryDs.groupByKey(entry => entry.pair_id).mapGroups {
      case (pairId, iter) =>
        val samples = iter.toSeq.groupBy(_.sample_id).map {
          case (sampleId, entries) =>
            val labels = entries.map(_.label).distinct
            require(labels.size == 1, "Each label must correspond to exactly one sample_id")
            val sampleMetadata =
              dnaseq
                .Manifest
                .entriesForSample(
                  manifest,
                  sampleId,
                  entries.map(_.toManifestEntry).iterator
                )
            (labels.head, sampleMetadata)
        }
        GroupedSampleMetadata(pairId, samples.toSeq)
    }
  }
}
