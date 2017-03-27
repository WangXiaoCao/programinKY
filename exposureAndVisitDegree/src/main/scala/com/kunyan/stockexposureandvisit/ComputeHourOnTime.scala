package com.kunyan.stockexposureandvisit

import com.kunyan.Util.TimeUtil
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/11/8.
  *
  * 定时计算前一天每小时的曝光度与访问量
  */
object ComputeHourOnTime {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_ComputeHourOnTime")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt
    val teleDataPath = args(2) //电信数据的路径

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取股票完整词库
    val stockDict = StockExposureAndVisit.getStockDict(jsonConfig, sqlContext)

    //获取mysql新闻数据
    val newsData = StockExposureAndVisit.readNewsData(jsonConfig, sqlContext, partitionNum)

    //预处理新闻数据
    val processData = StockExposureAndVisit.processNewsData(newsData, stockDict).cache()

    //生成时间的数组
    val day = TimeUtil.getLastDay
    val startTimeStamp = TimeUtil.getTimeStampDay(day)
    val endTimeStamp = startTimeStamp + 60L * 60 * 1000 * 24

    val hourList = sc.wholeTextFiles(teleDataPath + "*")
      .map(x => x._1.split("/")(6)).distinct()
      .filter(x => x.toLong > startTimeStamp && x.toLong < endTimeStamp)
      .collect().toList.sortBy(x => x)

    //对每小时进行计算与保存数据
    for (hour <- hourList) {

      val time = TimeUtil.getTimeByHour(hour)

      val visitData = sc.textFile(teleDataPath + hour)
        .map(_.split("\n")).flatMap(x => x)
        .filter(x => x.length != 0)

      //计算曝光度，按天保存（股票号，曝光度）
      val exposure = StockExposureAndVisit.computeExposure(processData, hour.toLong)
      StockExposureAndVisit.sendFinalResults(exposure.collect(), "exposureHour", time)

      //预处理电信访问数据并计算访问量，按天保存（股票号，访问量）
      val visitDataMap = StockExposureAndVisit.processVisitData(visitData)
      val visit = StockExposureAndVisit.computeVisit(processData, visitDataMap)
      StockExposureAndVisit.sendFinalResults(visit.collect(), "visitHour", time)

      //计算访问量的趋势，按天保存（股票号，访问趋势）
      val yesterdayData = sc.parallelize(StockExposureAndVisit.getLastHourData("visitHour", hour))
      val trendData = StockExposureAndVisit.computeTrend(visit, yesterdayData)
      StockExposureAndVisit.sendFinalResults(trendData.collect(), "trendHour", time)

    }

    sc.stop()
  }

}
