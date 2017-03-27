package com.kunyan.eventexposureandvisit

import com.kunyan.util.TimeUtil
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/8/12.
  *
  * 计算曝光度与访问量的历史数据（读取的电信数据按天存储）
  */
object ComputeByDay {

  /**
    * 整合各方法，获取数据，计算，保存
    *
    * @param sc SparkContext
    * @param jsonConfig 配置文件
    * @param sqlContext sql实例
    * @param startDay 起始日期
    * @param endDay 结束日期
    * @param path 电信访问数据保存的路径
    * @param num 计算曝光度时的时间范围
    * @note rowNum: 34
    */
  def run(sc: SparkContext, jsonConfig: JsonConfig, sqlContext: SQLContext,
          startDay: String, endDay: String, path: String, num: String): Unit = {

    //获取mysql中的新闻数据与事件数据，并缓存
    val eventData = EventExposureAndVisit.readEventData(sc, jsonConfig).cache()
    val newsData = EventExposureAndVisit.readNewsData(sc, jsonConfig, eventData).cache()

    //创建读取电信数据的日期list
    val startTimeStamp = TimeUtil.getTimeStampDay(startDay)
    val endTimeStamp = TimeUtil.getTimeStampDay(endDay)
    val dayList = List.range(startTimeStamp, endTimeStamp, 60L * 60 * 1000 * 24)

    for (date <- dayList) {

      //依次获取每个日期的电信访问数据
      val day = TimeUtil.getDay(date.toString)
      val visitData = sc.textFile(path + day + "-" + "PV")
        .map(_.split("\n"))
        .flatMap(x => x)
        .filter(x => x.length != 0)
        .map(_.split(","))

      val timeArray = Array("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
        "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")

      for (hour <- timeArray) {

        //获取该日期下的每个小时的电信数据并做预处理转换成map
        val simpleDate = day + "-" + hour
        val time = TimeUtil.getTimeStamp(simpleDate)

        val visitByHour = visitData
          .filter(x => x(0) == simpleDate)
          .map(x => x(1))
          .map(_.split("\t"))
          .filter(x => x.length == 2)
          .map(x => (x(0), x(1).toInt))
          .collect().toMap

        //计算曝光度
        val exposureDegree = EventExposureAndVisit.computeExposureDegree(eventData, newsData, time, num.toInt)

        //计算访问量
        val visitDegree = EventExposureAndVisit.computeVisitDegree(eventData, newsData, visitByHour)

        //整合两个结果
        val result = exposureDegree.join(visitDegree).collect()

        //保存到mysql
        EventExposureAndVisit.saveResult(jsonConfig, result, time)

      }

    }

  }

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_EventExposureHistoryByDay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取参数
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val startDay = args(1)
    val endDay = args(2)
    val path = args(3)
    val pasthour = args(4)

    //运行
    run(sc, jsonConfig, sqlContext, startDay, endDay, path, pasthour)

    sc.stop()
  }

}
