package com.kunyan.stockhotsavebyhour

<<<<<<< HEAD
import com.kunyan.Util.TimeUtil
=======
import com.kunyan.util.TimeUtil
>>>>>>> 93230f81aba1e172f94de8910369682e5aa3f348
import com.kunyan.stockexposureandvisit.StockExposureAndVisit
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/9/8.
  * 计算每小时的股票访问量，并保存到hdfs（读取的电信数据按天保存）
  * rowNum: 49
  */
object SaveByHourFromDay {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_SaveByHourFromDay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取参数
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt
    val startDate = args(2)
    val endDate = args(3)
    val teleDataPath = args(4)
    val outputPath = args(5)
    val noData = args(6).split(",")

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取股票完整词库
    val stockDict = StockExposureAndVisit.getStockDict(sc, jsonConfig).cache()

    //获取mysql新闻数据
    val newsData = StockExposureAndVisit.readNewsData(sc, jsonConfig, partitionNum).cache()

    //预处理股票数据
    val processData = StockExposureAndVisit.processNewsData(newsData, stockDict).cache()

    //创建日期list
    val startTimeStamp = TimeUtil.getTimeStampDay(startDate)
    val endTimeStamp = TimeUtil.getTimeStampDay(endDate)
    val dateList = List.range(startTimeStamp, endTimeStamp, 60L * 60 * 1000 * 24)

    //循环读取每天的数据
    for (date <- dateList) {

      val simpleDate = TimeUtil.getDay(date.toString)
      val visitDataDay = if (noData.contains(simpleDate)) {
        sc.parallelize(Array(""))
      } else {
        sc.textFile(teleDataPath + simpleDate + "-PV")
      }

      //每天数据中循环计算每小时的访问量并以小时保存到hdfs
      val hourArray = Array("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
        "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")

      for (hour <- hourArray) {

        val dayAndHour = simpleDate + "-" + hour

        val visitDataMap = visitDataDay
          .map(_.split(","))
          .filter(_.length == 2)
          .filter(x => x(0) == dayAndHour)
          .map(_(1).split("\t"))
          .filter(_.length == 2)
          .map(x =>(x(0), x(1).toInt))
          .reduceByKey(_ + _)
          .collect()
          .toMap

        val visitDegree = StockExposureAndVisit.computeVisit(processData, visitDataMap)

        visitDegree.sortBy(x => x._2, ascending = false).coalesce(1)
          .saveAsTextFile(outputPath + TimeUtil.getTimeStamp(dayAndHour))

      }

    }

    sc.stop()
  }

}
