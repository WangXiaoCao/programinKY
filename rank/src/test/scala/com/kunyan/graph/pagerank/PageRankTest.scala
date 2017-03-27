package com.kunyan.graph.pagerank

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by wangcao on 2016/10/17.
  */
class PageRankTest extends FlatSpec with Matchers {

  it should "parse correctly" in {

    val sparkConf = new SparkConf().setAppName("PageRankTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(Array("A B", "A C", "B A")).map(_.split(" "))

    val graph = PageRank.buildGraph(sc, data)
    graph.count should be (3)

    val score = PageRank.computing(sc, graph, 5)
    score.map(x => x._1).collect.mkString(",") should be ("A,B,C")

    val finalSore = PageRank.runPageRank(sc, data, 5)
    finalSore.map(x => x._1).collect.mkString(",") should be ("A,B,C")

  }

}
