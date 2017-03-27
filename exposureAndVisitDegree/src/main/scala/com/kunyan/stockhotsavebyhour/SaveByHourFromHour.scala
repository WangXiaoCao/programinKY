package com.kunyan.stockhotsavebyhour

<<<<<<< HEAD
import com.kunyan.Util.TimeUtil
=======
import com.kunyan.util.TimeUtil
>>>>>>> 93230f81aba1e172f94de8910369682e5aa3f348
import com.kunyan.stockexposureandvisit.StockExposureAndVisit
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangcao on 2016/9/7.
  * 计算每小时的股票访问量，并保存到hdfs（读取的电信数据按小时保存）
  * rowNum: 32
  */
object SaveByHourFromHour {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_SaveByHour")
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

    StockExposureAndVisit.initRedis(jsonConfig)

    //获取股票完整词库
    val stockDict = StockExposureAndVisit.getStockDict(sc, jsonConfig).cache()

    //获取mysql新闻数据
    val newsData = StockExposureAndVisit.readNewsData(sc, jsonConfig, partitionNum).cache()

    //获取电信数据
    val teleData = sc.wholeTextFiles(teleDataPath).cache()

    //预处理股票数据
    val processData = StockExposureAndVisit.processNewsData(newsData, stockDict).cache()

    //循环获取每小时的电信数据并计算热度
    val startTimeStamp = startDate.toLong
    val endTimeStamp = TimeUtil.getTimeStampDay(endDate)
    val hourList = List.range(startTimeStamp, endTimeStamp, 60L * 60 * 1000)

    for (hour <- hourList) {

      val visitData = teleData.map(x => (x._1.split("/")(6).toLong, x._2))
        .filter(x => x._1 == hour).values
        .map(_.split("\n")).flatMap(x => x)
        .filter(x => x.length != 0)

      val visitDataMap = StockExposureAndVisit.processVisitData(visitData)
      val visitDegree = StockExposureAndVisit.computeVisit(processData, visitDataMap)

      visitDegree.sortBy(x => x._2, ascending = false).coalesce(1).saveAsTextFile(outputPath + hour)

    }

    sc.stop()
  }

}
