package com.kunyan.eventexposureandvisit

import com.kunyan.util.{MysqlHandler, MySQLUtil}
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
  * Created by wangcao on 2016/8/10.
  *
  * 计算事件的曝光度与访问量
  * rowNum: 161
  */
object EventExposureAndVisit {

  /**
    * 读取事件及其相关的新闻id数据
    *
    * @param jsonConfig 配置
    * @return 事件名称与其相关新闻id
    * @note rowNum: 22
    */
  def readEventData(sc: SparkContext,
                    jsonConfig: JsonConfig): RDD[(String, Array[String])] = {

    MysqlHandler.init(jsonConfig.getValue("computeRelation", "jdbcUrlStock"))
    val mysqlOp = MysqlHandler.getInstance()
    val data = mysqlOp.readEventsData()

<<<<<<< HEAD
    sqlContext.read
      .jdbc(jsonConfig.getValue("computeRelation", "jdbcUrlStock"),
        "events_total", "end_time", 1L, currentTimestamp, 4, propStock)
      .registerTempTable("tempTableStock")

    val event = sqlContext.sql(s"select * from tempTableStock where is_valid = 1")

    event.map(row => {
      (row.getLong(0), row.getString(1), row.getString(3))
    }).filter(x => x._3.length > 0)
=======
    mysqlOp.closeConn()

    sc.parallelize(data)
      .filter(x => x._3.length > 0)
>>>>>>> 93230f81aba1e172f94de8910369682e5aa3f348
      .map(x => (x._1 + "-" + x._2, x._3.split(",")))

  }

  /**
    * 读取新闻id,平台号，url
    *
    * @param jsonConfig 配置文件
    * @return 新闻id,平台号，url,时间戳
    * @note rowNum: 19
    */
  def readNewsData(sc: SparkContext,
                   jsonConfig: JsonConfig,
                   eventData: RDD[(String, Array[String])]): RDD[(Int, String, String, Long)] = {

    MysqlHandler.init(jsonConfig.getValue("computeRelation", "jdbcUrlNews"))
    val mysqlOp = MysqlHandler.getInstance()
    val data = mysqlOp.readNewsData(eventData)

<<<<<<< HEAD
    val propNews = new Properties()
    propNews.setProperty("user", jsonConfig.getValue("computeRelation", "userNews"))
    propNews.setProperty("password", jsonConfig.getValue("computeRelation", "passwordNews"))
    propNews.setProperty("driver", "com.mysql.jdbc.Driver")

    sqlContext.read
      .jdbc(jsonConfig.getValue("computeRelation", "jdbcUrlNews"),
        "news_info", "news_time", 1L, currentTimestamp, 4, propNews)
      .registerTempTable("tempTableNews")

    val news = sqlContext.sql(s"select * from tempTableNews where type = 0 " +
      s"and id in ${newsIdArray.mkString("(", ",", ")")}")

    news.map(row => {
      (row.getInt(0), row.getString(3), row.getString(5), row.getLong(6))
    })
=======
    mysqlOp.closeConn()
>>>>>>> 93230f81aba1e172f94de8910369682e5aa3f348

    sc.parallelize(data)
  }

  /**
    * 处理数据，配对出每个事件所对应的新闻的url和平台
    *
    * @param event 读入的事件数据
    * @param news 读入的新闻数据
    * @return (event, Array(platform, url))
    * @note rowNum: 20
    */
  def processData (event: RDD[(String, Array[String])],
                   news: RDD[(Int, String, String)]): RDD[(String, Array[(String, String)])] = {

    val processEvent = event.flatMap(x => x._2.map(a => (a.toInt, x._1)))
      .reduceByKey(_+ "," +_)

    val processNews = news.map(x => (x._1, (x._2, x._3)))

    val joinData = processEvent.leftOuterJoin(processNews)
      .map(x => (x._2._1.split(","), x._2._2))
      .map(x => x._1.map(a => (a, x._2)))
      .flatMap(x => x)
      .map(x => {

        if (x._2.isEmpty) {
          (x._1, ("no", "no"))
        } else {
          (x._1, x._2.get)
        }

      })
      .groupByKey()
      .map(x => (x._1, x._2.toArray))

    joinData
  }

  /**
    * 预处理电信访问数据，计算相同URL被访问的次数（以小时存储的电信数据）
    *
    * @param data  电信访问数据
    * @return Map[url, 访问次数]
    * @note rowNum: 10
    */
  def processHourlyVisitData(data: RDD[String]): Map[String, Int] = {

    data.map(_.split(","))
      .filter(_.length == 2)
      .map(_(1).split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1).toInt))
      .reduceByKey(_ + _)
      .collect()
      .toMap

  }

  /**
    * 计算每个事件的曝光度
    *
    * @param event 读入的事件数据
    * @param news  读入的新闻的数据
    * @param time 时间戳
    * @param num 时间窗口
    * @return  新闻id,（url数目，platform)
    * @note rowNum: 19
    */
  def computeExposureDegree(event: RDD[(String, Array[String])],
                            news: RDD[(Int, String, String, Long)],
                            time: Long,
                            num: Int): RDD[(String, (Int, String))] = {

    val startTime = time - 60L * 60 * 1000 * num
    val endTime = startTime + 60L * 60 * 1000 * 1

    val lastHourNews = news.filter(x => x._4 < endTime && x._4 >= startTime)
      .map(x => (x._1, x._2, x._3))

    val exposure = processData(event, lastHourNews)

    val degree = exposure
      .map(line => {

        val event = line._1
        val usefulPlat = line._2.filter(a => a._1 != "no")
        val count = usefulPlat.length
        val platForm = usefulPlat.map(x => x._1).distinct.mkString(",")

        (event, (count, platForm))
      })

    degree
  }

  /**
    * 计算访问量
    *
    * @param event 读入事件数据
    * @param news  读入新闻数据
    * @param visitData  电信数据
    * @return  （event, visitDegree)
    * @note rowNum: 19
    */
  def computeVisitDegree(event: RDD[(String, Array[String])],
                         news: RDD[(Int, String, String, Long)],
                         visitData: Map[String, Int]): RDD[(String, Int)] = {

    val urlArray = visitData.keys.toArray
    val totalNews = news.map(x => (x._1, x._2, x._3))

    val visit = processData(event, totalNews)
      .map(line => {

        val event = line._1
        val count = line._2.map(_._2).map(x => {

          if (urlArray.contains(x)) {
            visitData(x)
          } else {
            0
          }

        }).sum

        (event, count)
      })

    visit
  }

  /**
    * 整合以上两个指标的计算（用于定时计算每小时）
    *
    * @param newsInfo 新闻数据
    * @param event 事件数据
    * @param visitData 访问量数据
    * @param time 时间戳
    * @param num 时间窗口
    * @return 事件及其对应的曝光热度和访问量
    * @note rowNum: 12
    */
  def process(newsInfo: RDD[(Int, String, String, Long)],
              event: RDD[(String, Array[String])],
              visitData: RDD[String],
              time: Long,
              num: Int): RDD[(String, ((Int, String), Int))] = {

    val exposureDegree = computeExposureDegree(event, newsInfo, time, num)

    val selectedVisitData = processHourlyVisitData(visitData)

    val visitDegree = computeVisitDegree(event, newsInfo, selectedVisitData)

    val total = exposureDegree.join(visitDegree)

    total
  }

  /**
    * 保存最后计算结果到mysql
    *
    * @param jsonConfig 配置文件
    * @param result 计算结果
    * @note rowNum: 22
    */
  def saveResult(jsonConfig: JsonConfig,
                 result: Array[(String, ((Int, String), Int))],
                 time: Long) = {

    val mysqlUrl = jsonConfig.getValue("computeRelation", "jdbcUrlNews")
    val mysqlConn = MySQLUtil.getConnect("com.mysql.jdbc.Driver", mysqlUrl)

    val sql = s"insert into event_exposure_and_visit " +
      s"(event_name, time, exposure, visit, platform, event_id)" +
      s"VALUES" +
      s"(?,?,?,?,?,?)"

    val prepNews = mysqlConn.prepareStatement(sql)

    result.foreach(row => {

      prepNews.setString(1, row._1.split("-")(1))
      prepNews.setLong(2, time)
      prepNews.setInt(3, row._2._1._1)
      prepNews.setInt(4, row._2._2)
      prepNews.setString(5, row._2._1._2)
      prepNews.setInt(6, row._1.split("-")(0).toInt)
      prepNews.addBatch()

    })

    prepNews.executeBatch()
    prepNews.close()

    mysqlConn.close()
  }

}
