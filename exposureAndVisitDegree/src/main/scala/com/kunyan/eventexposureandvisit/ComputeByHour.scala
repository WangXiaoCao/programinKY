package com.kunyan.eventexposureandvisit

import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
  * Created by wangcao on 2016/8/11.
  *
  * 计算曝光度与访问量的历史数据（读取的电信数据按小时存）
  */
object ComputeByHour {

  /**
    * 整合各方法， 获取数据，计算，保存
    *
    * @param sc SparkContext
    * @param jsonConfig 配置
    * @param sqlContext sql实例
    * @param path 电信数据保存的路径
    * @param startHour 起始小时
    * @param endHour 结束小时
    * @param num 计算曝光度时的时间范围
    */
  def run(sc: SparkContext, jsonConfig: JsonConfig, sqlContext: SQLContext,
          path: String, startHour: Long, endHour: Long, num: Int): Unit = {

    //获取mysql新闻数据与事件数据
    val eventData = EventExposureAndVisit.readEventData(sc, jsonConfig).cache()
    val newsData = EventExposureAndVisit.readNewsData(sc, jsonConfig, eventData).cache()

    //获取电信数据的时间列表
    val timeList = List.range(startHour, endHour, 60L * 60 * 1000)

    for (hour <- timeList) {

      //获取该小时数据
      val visitData = sc.textFile(path + hour.toString)

      //计算曝光度，访问量
      val result = EventExposureAndVisit
        .process(newsData, eventData, visitData, hour, num).collect()

      //保存
      EventExposureAndVisit.saveResult(jsonConfig, result, hour)

    }

  }

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_EventExposureHistoryByHour")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取参数
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val path = args(1)
    val startHour = args(2).toLong
    val length = args(3).toInt
    val num = args(4).toInt

   //运行
    run(sc, jsonConfig, sqlContext, path, startHour, length, num)

    sc.stop()
  }

}
