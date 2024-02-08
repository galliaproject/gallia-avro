// gallia-avro

// ===========================================================================
ThisBuild / organizationName     := "Gallia Project"
ThisBuild / organization         := "io.github.galliaproject" // *must* match groupId for sonatype
ThisBuild / organizationHomepage := Some(url("https://github.com/galliaproject"))
ThisBuild / startYear            := Some(2021)
ThisBuild / version              := "0.6.0-SNAPSHOT"
ThisBuild / description          := "A Scala library for data manipulation"
ThisBuild / homepage             := Some(url("https://github.com/galliaproject/gallia-avro"))
ThisBuild / licenses             := Seq("Apache 2" -> url("https://github.com/galliaproject/gallia-avro/blob/master/LICENSE"))
ThisBuild / developers           := List(Developer(
  id    = "anthony-cros",
  name  = "Anthony Cros",
  email = "contact.galliaproject@gmail.com",
  url   = url("https://github.com/anthony-cros")))
ThisBuild / scmInfo              := Some(ScmInfo(
  browseUrl  = url("https://github.com/galliaproject/gallia-core"),
  connection =     "scm:git@github.com:galliaproject/gallia-core.git"))

// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    name   := "gallia-avro",
    target := baseDirectory.value / "bin" / "avro")
  .settings(GalliaCommonSettings.mainSettings:_*)

// ===========================================================================
lazy val avroVersion  = "1.11.0"
lazy val slf4jVersion = "1.7.36"

// ---------------------------------------------------------------------------
libraryDependencies ++= Seq(
  "io.github.galliaproject" %% "gallia-core"   % version.value,
  "org.slf4j"               %  "slf4j-nop"     % slf4jVersion,
  "org.apache.avro"         %  "avro"          % avroVersion,
  "org.apache.avro"         %  "avro-compiler" % avroVersion /* just to deserialize IDL (as opposed to JSON)... */) // any way to serialize IDL (t220224102703)?

// ===========================================================================
sonatypeRepository     := "https://s01.oss.sonatype.org/service/local"
sonatypeCredentialHost :=         "s01.oss.sonatype.org"
publishMavenStyle      := true
publishTo              := sonatypePublishToBundle.value

// ===========================================================================

