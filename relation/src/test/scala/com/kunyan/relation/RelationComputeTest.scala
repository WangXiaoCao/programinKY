package com.kunyan.relation

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by wangcao on 2016/10/18.
  */
class RelationComputeTest extends FlatSpec with Matchers {

  it should "parse correctly" in {

    val conf = new SparkConf().setAppName("Warren_Relation").setMaster("local")
    val sc = new SparkContext(conf)

    val news = sc.parallelize(Array((1, "600000", "大数据", "IT")))
    val event = sc.parallelize(Array(("宝宝离婚", "1,2,3")))

    val property = RelationCompute.propertyAsKey(news, event)
    property.count should be (4)

    val newsId = RelationCompute.newsAsKey(news, event).take(1)
      .map(x => (x._1, x._2.mkString(","))).mkString(",")
    newsId should be ("2 -> ev_宝宝离婚")

    val relation = RelationCompute.getRelation(news, event)
    relation.count should be (4)

    sc.stop()
  }

}
