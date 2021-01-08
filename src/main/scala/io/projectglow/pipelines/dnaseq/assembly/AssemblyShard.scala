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

package io.projectglow.pipelines.dnaseq.assembly

import java.util.{Iterator => JIterator, List => JList}

import org.broadinstitute.hellbender.engine.MultiIntervalShard
import org.broadinstitute.hellbender.utils.SimpleInterval
import org.broadinstitute.hellbender.utils.downsampling.{ReadsDownsampler, ReadsDownsamplingIterator}
import org.broadinstitute.hellbender.utils.read.GATKRead

/**
 * A shard of reads over which we can assemble variants.
 *
 * @param iterator The reads in this shard.
 * @param callableRegion The shard boundaries within which valid variants may be called.
 * @param targetRegions The regions in this shard over which we should call variants.
 * @param paddedRegions The regions in this shard over which we should call variants, with padding.
 */
private[assembly] case class AssemblyShard private (
    iterator: JIterator[GATKRead],
    callableRegion: SimpleInterval,
    targetRegions: JList[SimpleInterval],
    paddedRegions: JList[SimpleInterval])
    extends MultiIntervalShard[GATKRead] {

  def getIntervals(): JList[SimpleInterval] = targetRegions
  def getPaddedIntervals(): JList[SimpleInterval] = paddedRegions
}
