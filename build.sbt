// gallia-avro

// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    organizationName     := "Gallia Project",
    organization         := "io.github.galliaproject", // *must* match groupId for sonatype
    name                 := "gallia-avro",
    version              := GalliaCommonSettings.CurrentGalliaVersion,
    homepage             := Some(url("https://github.com/galliaproject/gallia-avro")),
    scmInfo              := Some(ScmInfo(
        browseUrl  = url("https://github.com/galliaproject/gallia-avro"),
        connection =     "scm:git@github.com:galliaproject/gallia-avro.git")),
    licenses             := Seq("BSL 1.1" -> url("https://github.com/galliaproject/gallia-avro/blob/master/LICENSE")),
    description          := "A Scala library for data manipulation" )
  .settings(GalliaCommonSettings.mainSettings:_*)

// ===========================================================================
lazy val avroVersion  = "1.11.0"
lazy val slf4jVersion = "1.7.36"

// ---------------------------------------------------------------------------
libraryDependencies ++= Seq(
  "io.github.galliaproject" %% "gallia-core"   % GalliaCommonSettings.CurrentGalliaVersion,
  "org.slf4j"               %  "slf4j-nop"     % slf4jVersion,
  "org.apache.avro"         %  "avro"          % avroVersion,
  "org.apache.avro"         %  "avro-compiler" % avroVersion /* just to deserialize IDL (as opposed to JSON)... */) // any way to serialize IDL (t220224102703)?

// ===========================================================================
sonatypeRepository     := "https://s01.oss.sonatype.org/service/local"
sonatypeCredentialHost :=         "s01.oss.sonatype.org"
publishMavenStyle      := true
publishTo              := sonatypePublishToBundle.value

// ===========================================================================

