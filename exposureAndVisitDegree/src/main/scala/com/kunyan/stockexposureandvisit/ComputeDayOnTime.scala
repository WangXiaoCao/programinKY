package com.kunyan.stockexposureandvisit

import com.kunyan.util.TimeUtil
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/8/30.
  *
  * 实时计算前一天的曝光度，访问量与访问趋势
  */
object ComputeDayOnTime {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_ComputeDayOnTime")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取参数
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt
    val teleDataPath = args(2)

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取股票完整词库
    val stockDict = StockExposureAndVisit.getStockDict(sc, jsonConfig)

    //获取mysql新闻数据
    val newsData = StockExposureAndVisit.readNewsData(sc, jsonConfig, partitionNum)

    //获取hdfs电信访问数据
    val day = TimeUtil.getLastDay
    val hourArray = StockExposureAndVisit.getHourArray(day)
    var teleData = sc.parallelize(Array(""))

    for (hour <- hourArray) {

      val input = sc.textFile(teleDataPath + hour)
      teleData = teleData.union(input)

    }

    //计算昨天的每只股票的曝光度，访问量，访问量趋势，结果保存到redis
    StockExposureAndVisit.integration(sc, newsData, stockDict, teleData, day)

    sc.stop()
  }

}
