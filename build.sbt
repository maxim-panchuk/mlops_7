ThisBuild / scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.3.2" % "provided",
  "com.clickhouse" % "clickhouse-jdbc" % "0.4.6"
)
