package com.kunyan.stockexposureandvisit

import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/8/30.
  *
  * 计算每个月总的曝光度，访问量，与访问趋势
  */
object ComputeMonthHist {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_ComputeMonthHist")
    val sc = new SparkContext(conf)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取月份数据
    val date = args(1)
    val length = args(2).toInt
    val lastLength = args(3).toInt

    val visitData = StockExposureAndVisit.getDataByPeriods("visit", date, length)
    val weekVisit = sc.parallelize(visitData).reduceByKey(_ + _)
    StockExposureAndVisit.sendFinalResults(weekVisit.collect(), "visitMonth", date)

    val exposureData = StockExposureAndVisit.getDataByPeriods("exposure", date, length)
    val weekExposure = sc.parallelize(exposureData).reduceByKey(_ + _)
    StockExposureAndVisit.sendFinalResults(weekExposure.collect(), "exposureMonth", date)

    val trendData = sc.parallelize(StockExposureAndVisit.getLastPeriodsData("visitMonth", date, lastLength))
    val weekTrend = StockExposureAndVisit.computeTrend(weekVisit, trendData)
    StockExposureAndVisit.sendFinalResults(weekTrend.collect(), "trendMonth", date)

    sc.stop()
  }

}
