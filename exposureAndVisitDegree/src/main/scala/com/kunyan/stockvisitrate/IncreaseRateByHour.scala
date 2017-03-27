package com.kunyan.stockvisitrate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/9/7.
  * 计算每小时的股票访问量的增长率，并保存在hdfs
  * rowNum: 35
  */
object IncreaseRateByHour {

  /**
    * 预处理电信访问量数据
    *
    * @param data 访问量数据
    * @return （url, 访问次数）
    * @note rowNum: 6
    */
  def reprocess(data: RDD[String]): RDD[(String, Double)] = {

    data.map(_.replace("(", ""))
      .map(_.replace(")", ""))
      .map(_.split(","))
      .map(x => (x(0), x(1).toDouble))

  }

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_IncreaseRateByHour")
    val sc = new SparkContext(conf)

    val startTime = args(0).toLong
    val endTime = args(1).toLong
    val input = args(2)
    val output = args(3)

    val hourList = List.range(startTime, endTime, 60L * 60 * 1000)

    for (hour <- hourList) {

      val lastHour = hour - 60L * 60 * 1000
      val lastHourData = reprocess(sc.textFile(input + lastHour))
      val thisHourData = reprocess(sc.textFile(input + hour))

      val compare = thisHourData.join(lastHourData)
        .map(line => {

          val stock = line._1
          val now = line._2._1
          val justNow = line._2._2

          val rate = if (justNow == 0) {
            1
          } else {
            (now - justNow) / justNow
          }

          (stock, rate)
        })

      compare.coalesce(1).saveAsTextFile(output + hour)

    }

    sc.stop()
  }

}
