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

import io.projectglow.pipelines.PipelineStage
import io.projectglow.pipelines.mutseq.GroupedManifestEntry
import io.projectglow.pipelines.PipelineStage

/**
 * Defines a stage of the pipeline that can only run through a single sample
 * at a time.
 *
 * Classes that can only execute a sample at a time should extend [[SingleSampleStage]]
 * They can leverage {@link #getSampleMetadata} to determine the sample they are processing.
 */
trait SingleSampleStage extends PipelineStage with HasSampleMetadata {
  override def description(): String = s"${name()} ${getSampleMetadata.infoTag}"
}
