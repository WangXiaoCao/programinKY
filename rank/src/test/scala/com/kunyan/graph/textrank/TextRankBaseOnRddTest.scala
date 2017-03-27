package com.kunyan.graph.textrank

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by wangcao on 2016/10/17.
  */
class TextRankBaseOnRddTest extends FlatSpec with Matchers {

  it should "parse correctly" in {

    val sparkConf = new SparkConf().setAppName("TextRankBaseOnRddTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val text = Array("这", "简直", "太", "不可思议", "了", "太", "不可思议", "了", "不可思议", "了", "了")

    val range = TextRankBaseOnRdd.arrayRange(text, 1, 4)
    range.mkString(",") should be ("简直,太,不可思议,了")

    val relation = TextRankBaseOnRdd.buildRelation(text, 3)
    relation.length should be (11)

    val relationRdd = sc.parallelize(relation.map(x => (x._1, x._2)).toSeq)
      .groupByKey().map(line => (line._1, line._2.flatMap(x => x).toSet))
    val score = TextRankBaseOnRdd.computing(sc, relationRdd, 5 )
    score.map(x => x._1).mkString(",") should be ("太,不可思议,简直,了,这")

    val finalScore = TextRankBaseOnRdd.getTopHotWords(sc, text, 3, 5)
    finalScore.map(x => x._1).mkString(",") should be ("太,不可思议,简直,了,这")

  }

}
