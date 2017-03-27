package com.kunyan.stockexposureandvisit

import com.kunyan.Util.TimeUtil
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/11/14.
  *
  * 计算历史数据每小时的曝光度与访问量
  */
object ComputeHourHist {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_ComputeHourHist")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt
    val start = args(2).toLong
    val end = args(3).toLong

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取股票完整词库
    val stockDict = StockExposureAndVisit.getStockDict(jsonConfig, sqlContext)

    //获取mysql新闻数据
    val newsData = StockExposureAndVisit.readNewsData(jsonConfig, sqlContext, partitionNum)

    //预处理新闻数据
    val processData = StockExposureAndVisit.processNewsData(newsData, stockDict).cache()

    //生成时间数组
    val hourList = List.range(start, end, 60L * 60 * 1000)

    for (hour <- hourList) {

      val time = TimeUtil.getTimeByHour(hour.toString)

      //计算曝光度，按天保存（股票号，曝光度）
      val exposure = StockExposureAndVisit.computeExposure(processData, hour)
      StockExposureAndVisit.sendFinalResults(exposure.collect(), "exposureHour", time)

    }

    sc.stop()
  }

}
