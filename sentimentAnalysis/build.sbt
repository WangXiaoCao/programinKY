name := "sentimentAnalysis"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "kunyanData" at "http://61.147.114.81:9023/nexus/content/groups/Releases/"

libraryDependencies += "com.kunyan" % "nlpsuit-package" % "0.3.0.0"

libraryDependencies += "com.kunyan" % "pinyin4j" % "2.5.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.5" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.ansj" % "ansj_seg" % "0.9"

libraryDependencies += "org.json" % "json" % "20160212"

libraryDependencies += "org.graphstream" % "gs-core" % "1.1.2"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
    