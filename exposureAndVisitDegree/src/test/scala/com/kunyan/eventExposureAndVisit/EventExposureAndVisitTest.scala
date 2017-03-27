package com.kunyan.eventexposureandvisit

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
  * Created by wangcao on 2016/10/19.
  */
class EventExposureAndVisitTest extends FlatSpec with Matchers {

  it should "parse correctly" in {

    val conf = new SparkConf().setAppName("Warren_EventExposureAndVisit").setMaster("local")
    val sc = new SparkContext(conf)

    val events = sc.parallelize(Array(("油价暴涨", Array("1", "2", "3"))))
    val news = sc.parallelize(Array((1, "10", "url1")))

    val process = EventExposureAndVisit.processData(events, news)
      .map(x => (x._1, x._2.mkString(","))).collect().mkString(",")
    process should be ("(油价暴涨,(10,url1),(no,no),(no,no))")

    val originNews = sc.parallelize(Array((1, "10", "url1", 1476720000000L)))
    val exposure = EventExposureAndVisit.computeExposureDegree(events, originNews, 1476720000001L, 1)
      .collect().mkString(",")
    exposure should be ("(油价暴涨,(1,10))")

    val tele = sc.parallelize(Array("1,url1\t10", "2,url1\t5"))
    val proTele = EventExposureAndVisit.processHourlyVisitData(tele)
    val visit = EventExposureAndVisit.computeVisitDegree(events, originNews, proTele)
      .collect.mkString(",")
    visit should be ("(油价暴涨,15)")

    val total = EventExposureAndVisit.process(originNews, events, tele, 1476720000001L, 1)
      .collect().mkString(",")
    total should be ("(油价暴涨,((1,10),15))")

  }

}
