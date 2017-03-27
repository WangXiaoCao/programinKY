package com.kunyan.stockexposureandvisit

import com.kunyan.util.TimeUtil
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/8/30.
  *
  * 每周一凌晨实时计算上一周7天的总的曝光度，访问量，与访问趋势
  */
object ComputeWeekOnTime {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_ComputeWeekOnTime")
    val sc = new SparkContext(conf)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))

    StockExposureAndVisit.initRedis(jsonConfig)

    val date = TimeUtil.getLastWeek

    val visitData = StockExposureAndVisit.getDataByPeriods("visit", date, 6)
    val weekVisit = sc.parallelize(visitData).reduceByKey(_ + _)
    StockExposureAndVisit.sendFinalResults(weekVisit.collect(), "visitWeek", date)

    val exposureData = StockExposureAndVisit.getDataByPeriods("exposure", date, 6)
    val weekExposure = sc.parallelize(exposureData).reduceByKey(_ + _)
    StockExposureAndVisit.sendFinalResults(weekExposure.collect(), "exposureWeek", date)

    val trendData = sc.parallelize(StockExposureAndVisit.getLastPeriodsData("visitWeek", date, 7))
    val weekTrend = StockExposureAndVisit.computeTrend(weekVisit, trendData)
    StockExposureAndVisit.sendFinalResults(weekTrend.collect(), "trendWeek", date)

    sc.stop()
  }

}
