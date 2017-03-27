package com.kunyan.eventexposureandvisit

import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/8/18.
  *
  * 每小时计算上一小时的事件曝光度与访问量（定时计算）
  * rowNum: 18
  */
object ComputeOnTime {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_EventExposureAndVisit").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))

    //获取mysql新闻数据与事件数据
    val eventData = EventExposureAndVisit.readEventData(sc, jsonConfig).cache()
    val newsData = EventExposureAndVisit.readNewsData(sc, jsonConfig, eventData).cache()

    //newsData.take(50).foreach(println)
    val exposureDegree = EventExposureAndVisit.computeExposureDegree(eventData, newsData, 1487649600000L, 24)
    exposureDegree.take(50).foreach(println)

    sc.stop()
  }

}
