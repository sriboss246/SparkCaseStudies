ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.2"
val sparkVersion = "2.4.3"
lazy val root = (project in file("."))
  .settings(
    name := "DataGenProcess"
  )

libraryDependencies ++=Seq(
                       "org.apache.spark" %% "spark-core" % sparkVersion,
                        "org.apache.spark" %% "spark-sql" % sparkVersion,
                        "org.apache.spark" %% "spark-streaming" % sparkVersion,
                        "org.apache.spark" %% "spark-hive" % sparkVersion)
