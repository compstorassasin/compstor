import Dependencies._

ThisBuild / scalaVersion     := "2.12.14"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "CompStor"
ThisBuild / organizationName := "ASSASIN"

lazy val root = (project in file("."))
  .settings(
    name := "riscvstorage",
    libraryDependencies := Seq(
      log4jCore % Compile,
      log4jApi % Compile,
      sparkCore % Compile,
      sparkSql % Compile,
      scalaTest % Test
    )
  )

javacOptions ++= Seq("-Xlint")

unmanagedBase := baseDirectory.value / "src" / "main" / "c"
unmanagedBase := baseDirectory.value / "src" / "main" / "cpp"
unmanagedBase := baseDirectory.value / "src" / "main" / "python3"

