package com.kunyan.stockexposureandvisit

import com.kunyan.util.TimeUtil
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/8/17.
  *
  * 计算股票的曝光度与访问量（读取的电信数据为以时间戳命名的每小时的数据）
  */
object ComputeDayHistByHour {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_ComputeDayHistByHour")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取参数
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt
    val startDate = args(2)
    val dayNum = args(3).toInt
    val teleDataPath = args(4)

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取股票完整词库
    val stockDict = StockExposureAndVisit.getStockDict(sc, jsonConfig)

    //获取mysql新闻数据
    val newsData = StockExposureAndVisit.readNewsData(sc, jsonConfig, partitionNum)

    //获取hdfs电信访问数据
    val teleData = sc.wholeTextFiles(teleDataPath).map(x => (x._1.split("/")(6).toLong, x._2)).cache()

    //以“天”为单位循环计算每只股票的曝光度，访问量，访问量趋势，结果保存到redis
    val dayArray = StockExposureAndVisit.getPeriodsArray(startDate, dayNum, 1)

    for (day <- dayArray) {

      val startHour = TimeUtil.getTimeStampDay(day)
      val endHour = startHour + 60L * 60 * 1000 * 24

      val visitData = teleData
        .filter(x => x._1 >= startHour && x._1 < endHour).values
        .map(_.split("\n")).flatMap(x => x)
        .filter(x => x.length != 0)

      StockExposureAndVisit.integration(sc, newsData, stockDict, visitData, day)

    }

    sc.stop()
  }

}
