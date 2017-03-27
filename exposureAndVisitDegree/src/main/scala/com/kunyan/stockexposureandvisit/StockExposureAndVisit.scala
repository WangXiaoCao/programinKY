package com.kunyan.stockexposureandvisit

import java.util.{Properties, Date}
import com.kunyan.util.{MysqlHandler, TimeUtil}
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import redis.clients.jedis.Jedis
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
  * Created by wangcao on 2016/8/18.
  *
  * 计算股票的曝光度，访问量，与访问趋势
  * rowNum: 174
  */
object StockExposureAndVisit {

  var jedis: Jedis = null

  /**
    * 读取新闻url, 时间戳，关联的股票号
    *
    * @param jsonConfig 配置文件
    * @param partition 自定义分区数
    * @return （url, 时间戳，关联的股票号）
    * @note rowNum: 18
    */
  def readNewsData(sc: SparkContext,
                   jsonConfig: JsonConfig,
                   partition: Int): RDD[(String, String, Long)] = {

    MysqlHandler.init(jsonConfig.getValue("computeRelation", "jdbcUrlNews"))
    val mysqlOp = MysqlHandler.getInstance()
    val data = mysqlOp.getNewsData

    mysqlOp.closeConn()

    sc.parallelize(data).repartition(partition).filter(x => x._2.length > 0)

  }

  /**
    * 从mysql中获取股票代码列表
    *
    * @param jsonConfig 配置文件
    * @return （股票代码， 1）
    * @note rowNum: 19
    */
  def getStockDict (sc: SparkContext,
                    jsonConfig: JsonConfig): RDD[(String, Int)] = {

    MysqlHandler.init(jsonConfig.getValue("computeRelation", "jdbcUrlStockDict"))
    val mysqlOp = MysqlHandler.getInstance()
    val data = mysqlOp.getStockDict

    mysqlOp.closeConn()

    sc.parallelize(data)
  }

  /**
    * 处理新闻数据,将（url,time,stock1stock2..) => (stock,(url,time),(url,time))的形式
    * 并且补全整个股票词表，没有出现过的股票其value为null
    *
    * @param newsData 新闻数据url,time,stock1stock2..
    * @param stockDict 股票号词典
    * @return 转换成以股票号为key，与该股票相关的新闻组成的String为value的格式
    * @note rowNum: 16
    */
  def processNewsData(newsData: RDD[(String, String, Long)],
                      stockDict: RDD[(String, Int)]): RDD[(String, String)] = {

    val proNewsData =  newsData.map(x => (x._2, x._1 + ",," +x._3))
      .map(x => x._1.split(",").map(a => (a, x._2)))
      .flatMap(x => x)
      .reduceByKey(_+ "\t" +_)
      .rightOuterJoin(stockDict)
      .map(x => {

        if (x._2._1.isEmpty) {
          (x._1, null)
        } else {
          (x._1, x._2._1.get)
        }

      })

    proNewsData
  }

  /**
    * 计算曝光度
    *
    * @param processedData 处理后的新闻数据
    * @param day 计算日的0点0分的时间戳
    * @return （股票号，曝光度）
    * @note rowNum: 17
    */
  def computeExposure(processedData: RDD[(String, String)],
                      day: Long): RDD[(String, Int)] = {

    val startTime = day
    val endTime = day + 60L * 60 * 1000

    val exposureRate = processedData.map(x => {

      if (x._2 != null) {

        val news = x._2.split("\t").map(_.split(",,"))
          .map(a => a(1).toLong)
          .filter(a => a >= startTime && a <= endTime)

        val count = news.length

        (x._1, count)
      } else {
        (x._1, 0)
      }

    })

    exposureRate
  }

  /**
    * 预处理电信访问数据，计算相同URL被访问的次数
    *
    * @param data  电信访问数据
    * @return Map[url, 访问次数]
    * @note rowNum: 10
    */
  def processVisitData(data: RDD[String]): Map[String, Int] = {

    data.map(_.split(","))
      .filter(_.length == 2)
      .map(_(1).split("\t"))
      .filter(_.length == 2)
      .map(x =>(x(0), x(1).toInt))
      .reduceByKey(_ + _)
      .collect()
      .toMap
     
  }

  /**
    * 计算访问量
    *
    * @param processedData 处理后的新闻数据
    * @param visitData 处理后的电信访问数据
    * @return （股票号，访问量）
    * @note rowNum: 14
    */
  def computeVisit(processedData: RDD[(String, String)],
                   visitData: Map[String, Int]): RDD[(String, Int)] = {

    val visitRate = processedData.map(x => {

      if (x._2 != null) {

        val count = x._2.split("\t").map(a => a.split(",,"))
          .map(a => visitData.getOrElse(a(0), 0))
          .sum

        (x._1, count)
      } else {
        (x._1, 0)
      }

    })

    visitRate
  }

  /**
    * 初始化 redis
    *
    * @param jsonConfig 配置文件
    * @note rowNum: 9
    */
  def initRedis(jsonConfig: JsonConfig) = {

    val redisIp = jsonConfig.getValue("newsHot", "ip")
    val redisPort = jsonConfig.getValue("newsHot", "port").toInt
    val redisDB = jsonConfig.getValue("newsHot", "db").toInt
    val redisAuth = jsonConfig.getValue("newsHot", "auth")

    jedis = new Jedis(redisIp, redisPort)
    jedis.auth(redisAuth)
    jedis.select(redisDB)

  }

  /**
    * 获取上一个周期的数据
    *
    * @param category 数据的种类（redis中的key名组成部分）
    * @param day 这个周期的首天日期
    * @param num 整个周期的天数
    * @return （股票号，访问量）
    * @note rowNum: 9
    */
  def getLastPeriodsData(category: String, day: String, num: Int): Array[(String, Int)] = {

    val lastPeriod = TimeUtil.getDay((TimeUtil.getTimeStampDay(day) - 60L * 60 * 1000 * 24 * num).toString)

    jedis.zrangeWithScores(category + "_" + lastPeriod, 0, -1)
      .asScala.map(row => {

      val a = row.getScore.toInt
      val b = row.getElement

      (b, a)
    }).toArray

  }

  /**
    * 获取上一个周期的数据
    *
    * @param category 数据的种类（redis中的key名组成部分）
    * @param hour 这个周期的首天日期
    * @return （股票号，访问量）
    * @note rowNum: 9
    */
  def getLastHourData(category: String, hour: String): Array[(String, Int)] = {

    val lastPeriod = TimeUtil.getHour((hour.toLong - 60L * 60 * 1000).toString)

    jedis.zrangeWithScores(category + "_" + lastPeriod, 0, -1)
      .asScala.map(row => {

      val a = row.getScore.toInt
      val b = row.getElement

      (b, a)
    }).toArray

  }

  /**
    * 获取指定周期内的数据
    *
    * @param category 数据的种类（redis中的key名组成部分）
    * @param day 周期的首天日期
    * @param num 周期的长度
    * @return （股票号，访问量）
    * @note rowNum: 14
    */
  def getDataByPeriods(category: String, day: String, num: Int): Array[(String, Int)] = {

    val dayArray = getPeriodsArray(day, num, 1)

    val weekData = new ArrayBuffer[(String, Int)]()

    for (date <- dayArray) {

      val data = jedis.zrangeWithScores(category + "_" + date, 0, -1)
        .asScala.map(row => {

        val a = row.getScore.toInt
        val b = row.getElement

        (b, a)
      }).toArray

      weekData ++= data

    }

    weekData.toArray
  }

  /**
    * 计算访问量的趋势，比前一天多则为1，少则为-1，相同则为0
    *
    * @param today 计算当天的访问量
    * @param yesterday 前一天的访问量
    * @return （股票号，正负趋势）
    * @note rowNum: 14
    */
  def computeTrend(today: RDD[(String, Int)],
                   yesterday: RDD[(String, Int)]): RDD[(String, Int)] = {

    today.join(yesterday)
      .map(x => {

        val differ = x._2._1 - x._2._2

        if (differ > 0) {
          (x._1, 1)
        } else if (differ < 0) {
          (x._1, -1)
        } else {
          (x._1, 0)
        }

      })

  }

  /**
    * 将最终计算结果保存到redis中
    *
    * @param result 计算结果
    * @param name 表名头部
    * @param day 日期YYYY-MM-DD
    * @note rowNum: 9
    */
  def sendFinalResults(result: Array[(String, Int)],
                       name: String,
                       day: String): Unit = {

    val pipeline = jedis.pipelined()

    result.map(x => {

      pipeline.zadd(name + "_"  + day, x._2.toDouble, x._1)

    })

    pipeline.sync()
  }

  /**
    * 整合以上方法，计算股票的曝光度,访问量,访问趋势，并且分别在redis中按天保存成3张表
    *
    * @param sc SparkContext
    * @param newsData 新闻数据
    * @param stockDict 股票词库
    * @param visitData 电信数据
    * @param day 日期
    * @note rowNum: 15
    */
  def integration (sc:SparkContext,
                   newsData: RDD[(String, String, Long)],
                   stockDict: RDD[(String, Int)],
                   visitData: RDD[String],
                   day: String) = {

    //预处理新闻数据
    val processData = processNewsData(newsData, stockDict).cache()

    //计算曝光度，按天保存（股票号，曝光度）
    val exposure = computeExposure(processData, TimeUtil.getTimeStampDay(day))
    sendFinalResults(exposure.collect(), "exposure", day)

    //预处理电信访问数据并计算访问量，按天保存（股票号，访问量）
    val visitDataMap = processVisitData(visitData)
    val visit = computeVisit(processData, visitDataMap)
    sendFinalResults(visit.collect(), "visit", day)

    //计算访问量的趋势，按天保存（股票号，访问趋势）
    val yesterdayData = sc.parallelize(getLastPeriodsData("visit", day, 1))
    val trendData = computeTrend(visit, yesterdayData)
    sendFinalResults(trendData.collect(), "trend", day)

  }

  /**
    * 获取日期的数组
    *
    * @param day 初始日期
    * @param num 增加的天数
    * @param length 周期的长度
    * @return 日期的数组
    * @note rowNum: 9
    */
  def getPeriodsArray(day: String, num: Int, length: Int): Array[String] = {

    val dayTimeStamp = TimeUtil.getTimeStampDay(day)
    val dayArray = new ArrayBuffer[String]()

    for (i <- 0 to num) {

      val nextDay = TimeUtil.getDay((dayTimeStamp + 60L * 60 * 1000 * 24 * length * i).toString)
      dayArray += nextDay

    }

    dayArray.toArray
  }

  /**
    * 获取一天中每个小时0点分0秒的时间戳
    *
    * @param day 日期
    * @return 该天内的24个小时对应的时间戳
    * @note rowNum: 8
    */
  def getHourArray(day: String): Array[String] = {

    val hourArray = new ArrayBuffer[String]()

    for (i <- 0 to 23) {

      val nextHour = (TimeUtil.getTimeStampDay(day) + 60L * 60 * 1000 * i).toString
      hourArray += nextHour

    }

    hourArray.toArray
  }

}
