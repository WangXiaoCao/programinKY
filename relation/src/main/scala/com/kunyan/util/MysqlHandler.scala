package com.kunyan.util

import java.sql.{Connection, DriverManager}
import java.util.Date
import scala.collection.mutable.ArrayBuffer


/**
  * Created by QQ
  * Created on 2016/8/23.
  */
class MysqlHandler private(jdbcString: String) {

  private var conn: Connection = _

  /**
    * 关闭数据库连接
    * @note rowNum: 3
    */
  def closeConn(): Unit = {
    if (MysqlHandler != null) conn.close()
  }


  /**
    * 获取事件的id
    *
    * @param startTime 启示时间
    * @return 事件的id与新闻id
    * @note rowNum: 16
    */
  def getEventsData(startTime: Long): Array[(Int, String, Long, Long)] = {

    val state = conn.createStatement()

    val dataResult = state.executeQuery(s"select * from events_total " +
      s"where end_time > ${startTime - 3L * 24 * 60 * 60 * 1000}")

    val result = ArrayBuffer[(Int, String, Long, Long)]()

    while (dataResult.next()) {

      result.append(
        (dataResult.getInt("id"),
          dataResult.getString("related_news"),
          dataResult.getLong("start_time"),
          dataResult.getLong("end_time")
          )
      )

    }

    result.toArray
  }

  /**
    * 获取事件名称与相关新闻id
    *
    * @param time 读取的时间段
    * @return 事件名称与其相关新闻id
    * @author wangcao
    * @note rowNum: 17
    */
  def readEventsData(time: Long): Array[(String, String, Int)] = {

    val state = conn.createStatement()

    val currentTimestamp = new Date().getTime
    val lastTimeStamp = currentTimestamp - 1000 * 60 * 60 * time

    val dataResult = state.executeQuery(s"select * from events_total " +
      s"where end_time > $lastTimeStamp")

    val result = ArrayBuffer[(String, String, Int)]()

    while (dataResult.next()) {

      result.append(
        (dataResult.getString("event_name"),
          dataResult.getString("related_news"),
          dataResult.getInt("is_valid")
          )
      )

    }

    result.toArray
  }

  /**
    * 读取新闻id，与对应的industry, section, stock
    *
    * @param time 读取的时间段
    * @return 新闻id，与对应的industry, section, stock
    * @author wangcao
    * @note rowNum: 18
    */
  def readNewsData(time: Long): Array[(Int, String, String, String)] = {

    val state = conn.createStatement()

    val currentTimestamp = new Date().getTime
    val lastTimeStamp = currentTimestamp - 1000 * 60 * 60 * time

    val dataResult = state.executeQuery(s"select * from news_info where news_time < $currentTimestamp " +
      s"and news_time > $lastTimeStamp")

    val result = ArrayBuffer[(Int, String, String, String)]()

    while (dataResult.next()) {

      result.append(
        (dataResult.getInt("id"),
          dataResult.getString("industry"),
          dataResult.getString("section"),
          dataResult.getString("stock")
          )
      )

    }

    result.toArray
  }

}

object MysqlHandler {

  private var MysqlHandler: MysqlHandler = _

  /**
    * 判断jdbcurl中是否含有重连选项
    *
    * @param jdbcString jdbc字符串
    * @return 转换过的jdbc字符串
    * @note rowNum: 15
    */
  private def transformJdbcString(jdbcString: String): String = {

    if (jdbcString.contains("&autoReconnect=true")) {

      if (jdbcString.contains("&failOverReadOnly=false")) {

        jdbcString
      } else {

        s"$jdbcString&failOverReadOnly=false"
      }
    } else {

      if (jdbcString.contains("&failOverReadOnly=false")) {

        jdbcString.replace("&failOverReadOnly=false", "&autoReconnect=true&failOverReadOnly=false")
      } else {

        s"$jdbcString&autoReconnect=true&failOverReadOnly=false"
      }
    }
  }

  /**
    * 获取mysqlutil的实例
    *
    * @return MySQLUtil
    * @note rowNum: 5
    */
  def getInstance(): MysqlHandler = {

    if (MysqlHandler == null)
      println("init first")

    MysqlHandler
  }

  /**
    * 初始化mysqlUtil实例
    *
    * @param jdbcString jdbc字符串
    * @param reconnectTimes 断开链接后重连次数
    * @param initialTimeout 重连时间间隔
    * @note rowNum: 19
    */
  def init(jdbcString: String, reconnectTimes: Int = 5, initialTimeout: Int = 2): Unit = {

    if (MysqlHandler != null) {

      println("Already init")
      return
    }

    val jdbcURL = s"${transformJdbcString(jdbcString)}" +
      s"&maxReconnects=$reconnectTimes&initialTimeout=$initialTimeout"
    MysqlHandler = new MysqlHandler(jdbcURL)

    try {
      Class.forName("com.mysql.jdbc.Driver")
      MysqlHandler.conn = DriverManager.getConnection(jdbcURL)
    }
    catch {
      case e:Exception => e.printStackTrace()
    }

    sys.addShutdownHook {
      MysqlHandler.conn.close()
    }

  }

}
