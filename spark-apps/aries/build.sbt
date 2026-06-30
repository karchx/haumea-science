name := "spark-aries"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.0" % "provided",
  "org.typelevel" %% "cats-effect" % "3.7.0",
  "com.github.Keplerlabs-M42" % "cds-healpix-java" % "7bfe155334ab52e28e50c46415e90a26ae173631"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
