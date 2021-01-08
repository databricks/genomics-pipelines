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

package io.projectglow.pipelines.joint

import io.projectglow.pipelines.{NotebookParam, PipelineBaseTest}

class JointGenotypingPipelineSuite extends PipelineBaseTest {

  test("check params") {
    val pipeline = JointGenotypingPipeline
    val userVisibleParams = pipeline.getVisibleParams
    val actualUserVisibleNames = userVisibleParams.map {
      case NotebookParam(name, _, default, dataType, _) =>
        (name, default, dataType)
    }

    val stringType = classOf[String].getSimpleName
    val booleanType = classOf[Boolean].getSimpleName

    val expectedUserVisibleNames = Seq(
      ("manifest", Some(""), stringType),
      ("output", None, stringType),
      ("gvcfDeltaOutput", Some(""), stringType),
      ("exportVCF", Some("false"), booleanType),
      ("exportVCFAsSingleFile", Some("true"), booleanType),
      ("targetedRegions", Some(""), stringType),
      ("saveMode", Some("overwrite"), stringType),
      ("vcfCompressionCodec", Some("bgzf"), stringType),
      ("replayMode", Some("skip"), stringType),
      ("performValidation", Some("false"), booleanType),
      ("validationStringency", Some("STRICT"), stringType)
    ).sortBy(_._1)

    assert(
      expectedUserVisibleNames sameElements actualUserVisibleNames,
      (expectedUserVisibleNames, actualUserVisibleNames)
    )

    val requiredParams = pipeline.getVisibleParams.filter(_.defaultValue.isEmpty)
    val actualParamNames = requiredParams.map(_.name)
    val expectedParamNames = Seq("output")
    assert(actualParamNames sameElements expectedParamNames, (actualParamNames, expectedParamNames))
  }

  test("removed emitAllSites and genotypeGivenAlleles") {
    val pipeline = JointGenotypingPipeline
    val params = Map(
      "emitAllSites" -> "true",
      "genotypeGivenAlleles" -> "true",
      "performValidation" -> "true", // User-visible
      "referenceGenomeFastaPath" -> "" // Not user-visible
    )
    val e = intercept[IllegalArgumentException](pipeline.init(params))
    assert(
      e.getMessage
        .contains("The following parameters are unrecognized: emitAllSites, genotypeGivenAlleles.")
    )
  }
}
