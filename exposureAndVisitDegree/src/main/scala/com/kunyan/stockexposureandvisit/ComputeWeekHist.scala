package com.kunyan.stockexposureandvisit

import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/8/30.
  *
  * 计算每周的曝光度，访问量与访问趋势
  */
object ComputeWeekHist {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_ComputeWeekHist")
    val sc = new SparkContext(conf)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取历史数据中每周的周一日期，组成数组
    val startDay = StockExposureAndVisit.getPeriodsArray(args(1), 2, 7)

    //计算每周的访问量，曝光度与访问趋势
    startDay.foreach(date => {

      val visitData = StockExposureAndVisit.getDataByPeriods("visit", date, 6)
      val weekVisit = sc.parallelize(visitData).reduceByKey(_ + _)
      StockExposureAndVisit.sendFinalResults(weekVisit.collect(), "visitWeek", date)

      val exposureData = StockExposureAndVisit.getDataByPeriods("exposure", date, 6)
      val weekExposure = sc.parallelize(exposureData).reduceByKey(_ + _)
      StockExposureAndVisit.sendFinalResults(weekExposure.collect(), "exposureWeek", date)

      val trendData = sc.parallelize(StockExposureAndVisit.getLastPeriodsData("visitWeek", date, 7))
      val weekTrend = StockExposureAndVisit.computeTrend(weekVisit, trendData)
      StockExposureAndVisit.sendFinalResults(weekTrend.collect(), "trendWeek", date)

    })

    sc.stop()
  }

}
