import scala.sys.process._

import complete.DefaultParsers._
import org.apache.commons.lang3.StringUtils
import sbt.Tests._
import sbt.Keys._
import sbt.librarymanagement.ModuleID
import sbt.nio.Keys._



lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"

lazy val spark3 = "3.0.0"
lazy val spark2 = "2.4.5"

lazy val sparkVersion = settingKey[String]("sparkVersion")
ThisBuild / sparkVersion := sys.env.getOrElse("SPARK_VERSION", spark3)

def majorVersion(version: String): String = {
  StringUtils.ordinalIndexOf(version, ".", 1) match {
    case StringUtils.INDEX_NOT_FOUND => version
    case i => version.take(i)
  }
}

def majorMinorVersion(version: String): String = {
  StringUtils.ordinalIndexOf(version, ".", 2) match {
    case StringUtils.INDEX_NOT_FOUND => version
    case i => version.take(i)
  }
}

ThisBuild / organization := "io.projectglow"
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"
ThisBuild / publish / skip := true

ThisBuild / organizationName := "The Glow Authors"
ThisBuild / startYear := Some(2019)
ThisBuild / licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))

// Compile Java sources before Scala sources, so Scala code can depend on Java
// but not vice versa
Compile / compileOrder := CompileOrder.JavaThenScala

// Test concurrency settings
// Tests are run serially in one or more forked JVMs. This setup is necessary because the shared
// Spark session used by many tasks cannot be used concurrently.
val testConcurrency = 1
Test / fork := true
concurrentRestrictions in Global := Seq(
  Tags.limit(Tags.ForkedTestGroup, testConcurrency)
)

def groupByHash(tests: Seq[TestDefinition]): Seq[Tests.Group] = {
  tests
    .groupBy(_.name.hashCode % testConcurrency)
    .map {
      case (i, groupTests) =>
        val options = ForkOptions()
          .withRunJVMOptions(Vector("-Dspark.ui.enabled=false", "-Xmx1024m"))

        Group(i.toString, groupTests, SubProcess(options))
    }
    .toSeq
}

lazy val mainScalastyle = taskKey[Unit]("mainScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
// testGrouping cannot be set globally using the `Test /` syntax since it's not a pure value
lazy val commonSettings = Seq(
  mainScalastyle := scalastyle.in(Compile).toTask("").value,
  testScalastyle := scalastyle.in(Test).toTask("").value,
  testGrouping in Test := groupByHash((definedTests in Test).value),
  test in Test := ((test in Test) dependsOn mainScalastyle).value,
  test in Test := ((test in Test) dependsOn testScalastyle).value,
  test in Test := ((test in Test) dependsOn scalafmtCheckAll).value,
  test in Test := ((test in Test) dependsOn (headerCheck in Compile)).value,
  test in Test := ((test in Test) dependsOn (headerCheck in Test)).value,
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    // Assembly jar is not executable
    case p if p.toLowerCase.contains("manifest.mf") =>
      MergeStrategy.discard
    case _ =>
      // Be permissive for other files
      MergeStrategy.first
  },
  scalacOptions += "-target:jvm-1.8",
  resolvers += "Apache Snapshots" at "https://repository.apache.org/snapshots/"
)

def runCmd(args: File*): Unit = {
  args.map(_.getPath).!!
}

lazy val sparkDependencies = settingKey[Seq[ModuleID]]("sparkDependencies")
lazy val providedSparkDependencies = settingKey[Seq[ModuleID]]("providedSparkDependencies")
lazy val testSparkDependencies = settingKey[Seq[ModuleID]]("testSparkDependencies")

ThisBuild / sparkDependencies := Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value,
  "org.apache.spark" %% "spark-core" % sparkVersion.value,
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value,
  "org.apache.spark" %% "spark-sql" % sparkVersion.value
)

ThisBuild / providedSparkDependencies := sparkDependencies.value.map(_ % "provided")
ThisBuild / testSparkDependencies := sparkDependencies.value.map(_ % "test")

lazy val testDependencies = settingKey[Seq[ModuleID]]("testCoreDependencies")
ThisBuild / testDependencies := Seq(
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
  "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.6.2",
  "org.xerial" % "sqlite-jdbc" % "3.20.1" % "test"
).map(_.exclude("org.apache.logging.log4j", "log4j-core"))

val bdgUtilsVersion = "0.3.0"
val adamVersion = "0.32.0"

lazy val dependencies = settingKey[Seq[ModuleID]]("coreDependencies")
ThisBuild / dependencies := (providedSparkDependencies.value ++ testDependencies.value ++ Seq(
  "org.seqdoop" % "hadoop-bam" % "7.9.2",
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.jdbi" % "jdbi" % "2.63.1",
  "com.github.broadinstitute" % "picard" % "2.21.9",
  // Fix versions of libraries that are depended on multiple times
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "io.netty" % "netty" % "3.9.9.Final",
  "io.netty" % "netty-all" % "4.1.17.Final",
  "com.github.samtools" % "htsjdk" % "2.21.2",
  "org.yaml" % "snakeyaml" % "1.16",
  ("org.broadinstitute" % "gatk" % "4.1.4.1")
    .exclude("biz.k11i", "xgboost-predictor")
    .exclude("com.esotericsoftware", "kryo")
    .exclude("com.esotericsoftware", "reflectasm")
    .exclude("com.github.jsr203hadoop", "jsr203hadoop")
    .exclude("com.google.cloud", "google-cloud-nio")
    .exclude("com.google.cloud.bigdataoss", "gcs-connector")
    .exclude("com.intel", "genomicsdb")
    .exclude("com.intel.gkl", "gkl")
    .exclude("com.opencsv", "opencsv")
    .exclude("commons-io", "commons-io")
    .exclude("gov.nist.math.jama", "gov.nist.math.jama")
    .exclude("it.unimi.dsi", "fastutil")
    .exclude("org.aeonbits.owner", "owner")
    .exclude("org.apache.commons", "commons-lang3")
    .exclude("org.apache.commons", "commons-math3")
    .exclude("org.apache.commons", "commons-collections4")
    .exclude("org.apache.commons", "commons-vfs2")
    .exclude("org.apache.hadoop", "hadoop-client")
    .exclude("org.apache.spark", s"spark-mllib_2.11")
    .exclude("org.bdgenomics.adam", s"adam-core-spark2_2.11")
    .exclude("org.broadinstitute", "barclay")
    .exclude("org.broadinstitute", "hdf5-java-bindings")
    .exclude("org.broadinstitute", "gatk-native-bindings")
    .exclude("org.broadinstitute", "gatk-bwamem-jni")
    .exclude("org.broadinstitute", "gatk-fermilite-jni")
    .exclude("org.objenesis", "objenesis")
    .exclude("org.ojalgo", "ojalgo")
    .exclude("org.ojalgo", "ojalgo-commons-math3")
    .exclude("org.reflections", "reflections")
    .exclude("org.seqdoop", "hadoop-bam")
    .exclude("org.xerial", "sqlite-jdbc")
    .exclude("com.github.fommil.netlib", "*")
  ,
  "org.broadinstitute" % "gatk-bwamem-jni" % "1.0.5",
  "org.bdgenomics.adam" %% "adam-apis-spark3" % adamVersion,
  "io.projectglow" %% "glow-spark3" % "0.6.0",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "com.google.guava" % "guava" % "15.0",
  "io.delta" %% "delta-core" % "0.7.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.6.2",
)).map(_
  .exclude("com.google.code.findbugs", "jsr305")
  .exclude("org.apache.logging.log4j", "log4j-api")
  .exclude("org.apache.logging.log4j", "log4j-core")
  .exclude("distlib", "distlib"))

lazy val scalaLoggingDependency = settingKey[ModuleID]("scalaLoggingDependency")
ThisBuild / scalaLoggingDependency := {
  (ThisBuild / scalaVersion).value match {
    case `scala211` => "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
    case `scala212` => "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
    case _ =>
      throw new IllegalArgumentException(
        "Only supported Scala versions are: " + Seq(scala211, scala212))
  }
}

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := s"glow-pipelines",
    publish / skip := false,
    // Adds the Git hash to the MANIFEST file. We set it here instead of relying on sbt-release to
    // do so.
    packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes("Git-Release-Hash" -> currentGitHash(baseDirectory.value)),
    bintrayRepository := "glow",
    libraryDependencies ++= dependencies.value :+ scalaLoggingDependency.value,
    Compile / unmanagedSourceDirectories +=
    baseDirectory.value / "src" / "main" / "shim" / majorMinorVersion(sparkVersion.value),
    Test / unmanagedSourceDirectories +=
    baseDirectory.value / "src" / "test" / "shim" / majorMinorVersion(sparkVersion.value),
  )

/**
 * @param dir The base directory of the Git project
 * @return The commit of HEAD
 */
def currentGitHash(dir: File): String = {
  Process(
    Seq("git", "rev-parse", "HEAD"),
    // Set the working directory for Git to the passed in directory
    Some(dir)
  ).!!.trim
}

lazy val sparkClasspath = taskKey[String]("sparkClasspath")
lazy val sparkHome = taskKey[String]("sparkHome")

// Publish to Bintray
ThisBuild / description := "An open-source toolkit for large-scale genomic analysis"
ThisBuild / homepage := Some(url("https://projectglow.io"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/projectglow/glow"),
    "scm:git@github.com:projectglow/glow.git"
  )
)
ThisBuild / developers := List(
  Developer(
    "henrydavidge",
    "Henry Davidge",
    "hhd@databricks.com",
  url("https://github.com/henrydavidge")),
  Developer(
    "karenfeng",
    "Karen Feng",
    "karen.feng@databricks.com",
    url("https://github.com/karenfeng")),
  Developer(
    "kianfar77",
    "Kiavash Kianfar",
    "kiavash.kianfar@databricks.com",
    url("https://github.com/kianfar77"))
)

ThisBuild / pomIncludeRepository := { _ =>
  false
}
ThisBuild / publishMavenStyle := true

ThisBuild / bintrayOrganization := Some("projectglow")
ThisBuild / bintrayRepository := "glow"

lazy val stableVersion = settingKey[String]("Stable version")
ThisBuild / stableVersion := "0.1.0"

lazy val stagedRelease = (project in file("core/src/test"))
  .settings(
    commonSettings,
    resourceDirectory in Test := baseDirectory.value / "resources",
    scalaSource in Test := baseDirectory.value / "scala",
    unmanagedSourceDirectories in Test += baseDirectory.value / "shim" / majorMinorVersion(
      sparkVersion.value),
    libraryDependencies ++= testSparkDependencies.value ++ testDependencies.value :+
    "io.projectglow" %% s"glow-spark${majorVersion(sparkVersion.value)}" % stableVersion.value % "test",
    resolvers := Seq("bintray-staging" at "https://dl.bintray.com/projectglow/glow"),
    org
      .jetbrains
      .sbt
      .extractors
      .SettingKeys
      .sbtIdeaIgnoreModule := true // Do not import this SBT project into IDEA
  )

import ReleaseTransformations._

// Don't use sbt-release's cross facility
releaseCrossBuild := false

lazy val changePySparkVersion = taskKey[Unit]("changePySparkVersion ")
changePySparkVersion := {
  "conda remove -n glow pyspark" !; s"conda install -n glow pyspark=$spark2" !
}

lazy val updateCondaEnv = taskKey[Unit]("Update Glow Env To Latest Version")
updateCondaEnv := {
  "conda env update -f python/environment.yml" !
}

def crossReleaseStep(step: ReleaseStep, requiresPySpark: Boolean): Seq[ReleaseStep] = {
  val updateCondaEnvStep = releaseStepCommandAndRemaining(
    if (requiresPySpark) "updateCondaEnv" else "")
  val changePySparkVersionStep = releaseStepCommandAndRemaining(
    if (requiresPySpark) "changePySparkVersion" else "")

  Seq(
    updateCondaEnvStep,
    releaseStepCommandAndRemaining(s"""set ThisBuild / sparkVersion := "$spark3""""),
    releaseStepCommandAndRemaining(s"""set ThisBuild / scalaVersion := "$scala212""""),
    step,
    changePySparkVersionStep,
    releaseStepCommandAndRemaining(s"""set ThisBuild / sparkVersion := "$spark2""""),
    releaseStepCommandAndRemaining(s"""set ThisBuild / scalaVersion := "$scala211""""),
    step,
    releaseStepCommandAndRemaining(s"""set ThisBuild / scalaVersion := "$scala212""""),
    step,
    updateCondaEnvStep
  )
}

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean
) ++
crossReleaseStep(runTest, true) ++
Seq(
  setReleaseVersion,
  updateStableVersion,
  commitReleaseVersion,
  commitStableVersion,
  tagRelease
) ++
crossReleaseStep(publishArtifacts, false) ++
crossReleaseStep(releaseStepCommandAndRemaining("stagedRelease/test"), false) ++
Seq(
  setNextVersion,
  commitNextVersion
)
