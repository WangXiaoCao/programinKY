package com.kunyan.stockexposureandvisit

import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/8/18.
  *
  * 计算股票的曝光度与访问量（读取的电信数据为以"YYYY-MM-DD-PV"命名的每天的的数据）
  */
object ComputeDayHistByDay {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_ComputeDayHistByDay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取参数
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt
    val startDate = args(2)
    val dayNum = args(3).toInt
    val teleDataPath = args(4)
    val noData = sc.broadcast(args(5).split(",")).value

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取股票完整词库
    val stockDict = StockExposureAndVisit.getStockDict(sc, jsonConfig)

    //获取mysql新闻数据
    val newsData = StockExposureAndVisit.readNewsData(sc, jsonConfig, partitionNum).cache()

    //循环读取hdfs上每日的电信访问数据,计算结果保存到redis
    val dayArray = StockExposureAndVisit.getPeriodsArray(startDate, dayNum, 1)
    val dayArrayBr = sc.broadcast(dayArray).value

    for (day <- dayArrayBr) {

      val visitData =
        if (!noData.contains(day)) {
          sc.textFile(teleDataPath + day + "-PV").filter(x => x != "")
        } else {
          sc.parallelize(Array(""))
        }

      StockExposureAndVisit.integration(sc, newsData, stockDict, visitData, day)

    }

    sc.stop()
  }

}
