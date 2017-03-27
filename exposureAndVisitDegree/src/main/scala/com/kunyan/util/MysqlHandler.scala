package com.kunyan.util

import java.sql.{Connection, DriverManager}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer


/**
  * Created by QQ
  * Created on 2016/8/23.
  */
class MysqlHandler private(jdbcString: String) {

  private var conn: Connection = _

  /**
    * 关闭数据库连接
    */
  def closeConn(): Unit = {
    if (MysqlHandler != null) conn.close()
  }

  /**
    * 获取事件名称与相关新闻id
    *
    * @return 事件名称与其相关新闻id
    * @author wangcao
    * @note rowNum: 17
    */
  def readEventsData: Array[(Int, String, String)] = {

    val state = conn.createStatement()

    val dataResult = state.executeQuery(s"select * from events_total where is_valid = 1")

    val result = ArrayBuffer[(Int, String, String)]()

    while (dataResult.next()) {

      result.append(
        (dataResult.getInt("id"),
          dataResult.getString("event_name"),
          dataResult.getString("related_news")
          )
      )

    }

    result.toArray
  }


  /**
    * 读取新闻id，与对应的industry, section, stock
    *
    * @return 新闻id，与对应的industry, section, stock
    * @author wangcao
    * @note rowNum: 18
    */
  def readNewsData(eventData: RDD[(String, Array[String])]): Array[(Int, String, String, Long)] = {

    val state = conn.createStatement()
    val newsIdArray = eventData.map(_._2).flatMap(x => x).distinct.collect()

    val dataResult = state.executeQuery(s"select * from news_info where type = 0 " +
      s"and id in ${newsIdArray.mkString("(", ",", ")")}")

    val result = ArrayBuffer[(Int, String, String, Long)]()

    while (dataResult.next()) {

      result.append(
        (dataResult.getInt("id"),
          dataResult.getString("platform"),
          dataResult.getString("url"),
          dataResult.getLong("news_time")
          )
      )

    }

    result.toArray
  }

  /**
    * 获取新闻的URL，股票号，时间戳
    *
    * @return （URL，股票号，时间戳）
    * @author wangcao
    * @note rowNum: 14
    */
  def getNewsData: Array[(String, String, Long)] = {

    val state = conn.createStatement()

    val dataResult = state.executeQuery("select * from news_info where type = 0")

    val result = ArrayBuffer[(String, String, Long)]()

    while (dataResult.next()) {

      result.append(
        (dataResult.getString("url"),
          dataResult.getString("stock"),
          dataResult.getLong("news_time")
          )
      )

    }

    result.toArray
  }

  /**
    * 获取股票词典
    *
    * @return （股票号，1）
    * @author wangcao
    * @note rowNum: 14
    */
  def getStockDict: Array[(String, Int)] = {

    val state = conn.createStatement()

    val dataResult = state.executeQuery(s"select SYMBOL from bt_stcode " +
      s"where (EXCHANGE = '001002' or EXCHANGE = '001003') " +
      s"and SETYPE = '101' and CUR = 'CNY' and ISVALID = 1 " +
      s"and LISTSTATUS <> '2'")

    val result = ArrayBuffer[(String)]()

    while (dataResult.next()) {

      result.append(
        dataResult.getString("SYMBOL")
      )

    }

    result.toArray.map(x => (x, 1))
  }

}

object MysqlHandler {

  private var MysqlHandler: MysqlHandler = _

  /**
    * 判断jdbcurl中是否含有重连选项
    *
    * @param jdbcString jdbc字符串
    * @return 转换过的jdbc字符串
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
