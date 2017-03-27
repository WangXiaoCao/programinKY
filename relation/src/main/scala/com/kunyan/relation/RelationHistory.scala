package com.kunyan.relation

import java.sql.Connection
import java.util.Properties
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.kunyan.util.TimeUtil

/**
  * Created by wangcao on 2017/2/10.
  *
  * 计算所有历史关系图谱
  */
object RelationHistory {

  /**
    * 读取指定时间点的过去24个小时的新闻及其属性的数据
    *
    * @param jsonConfig 配置文件
    * @param sqlContext SQL实例
    * @param currentTimestamp: 指定的当前时间（一天的0点）
    * @param partitionNum 分区数
    * @return 新闻id, 及其股票，概念，行业的三类属性
    * @note rowNum: 21
    */
  def readNewsData(jsonConfig: JsonConfig,
                   sqlContext: SQLContext,
                   currentTimestamp: Long,
                   partitionNum: Int): RDD[(Int, String, String, String)] = {

    //过去多少个小时（24）
    val time = jsonConfig.getValue("computeRelation", "newsTimeWindow").toLong
    val lastTimeStamp = currentTimestamp - 1000 * 60 * 60 * time

    val propNews = new Properties()
    propNews.setProperty("user", jsonConfig.getValue("computeRelation", "userNews"))
    propNews.setProperty("password", jsonConfig.getValue("computeRelation", "passwordNews"))
    propNews.setProperty("driver", "com.mysql.jdbc.Driver")

    sqlContext.read
      .jdbc(jsonConfig.getValue("computeRelation", "jdbcUrlNews"),
        "news_info", "news_time", 1L, currentTimestamp, 4, propNews)
      .registerTempTable("tempTableNews")

    val event = sqlContext.sql(s"select * from tempTableNews where news_time < $currentTimestamp " +
      s"and news_time > $lastTimeStamp")

    event.map(row => {
      (row.getInt(0), row.getString(7), row.getString(8), row.getString(9))
    }).repartition(partitionNum)
      .filter(x => x._2.length + x._3.length + x._4.length > 0)

  }

  /**
    * 读取事件及其相关的新闻id数据
    *
    * @param jsonConfig 配置
    * @param sqlContext SQL实例
    * @param currentTimestamp: 指定的当前时间（一天的0点）
    * @param partitionNum 分区数
    * @return 事件名称与其相关新闻id
    * @note rowNum: 18
    */
  def readEventData(jsonConfig: JsonConfig,
                    sqlContext: SQLContext,
                    currentTimestamp: Long,
                    partitionNum: Int): RDD[(String, String)] = {

    //过去多少个小时（24）
    val time = jsonConfig.getValue("computeRelation", "eventTimeWindow").toLong
    val lastTimeStamp = currentTimestamp - 1000 * 60 * 60 * time

    val propStock = new Properties()
    propStock.setProperty("user", jsonConfig.getValue("computeRelation", "userStock"))
    propStock.setProperty("password", jsonConfig.getValue("computeRelation", "passwordStock"))
    propStock.setProperty("driver", "com.mysql.jdbc.Driver")

    sqlContext.read
      .jdbc(jsonConfig.getValue("computeRelation", "jdbcUrlStock"),
        "events_total", "end_time", 1L, currentTimestamp, 4, propStock)
      .registerTempTable("tempTableStock")

    val event = sqlContext.sql(s"select * from tempTableStock " +
      s"where end_time > $lastTimeStamp and end_time < $currentTimestamp")

    event.map(row => {
      (row.getString(1), row.getString(3), row.getInt(11))
    })
      .repartition(partitionNum)
      .filter(x => x._2.length > 0)
      .filter(x => x._3 == 1)
      .map(x => (x._1, x._2))

  }

  /**
    * 保存计算结果到mysql中
    *
    * @param jsonConfig 配置文件
    * @param sqlContext sql实例
    * @param mysqlConn sql连接
    * @param newsData 从mysql中读入的新闻数据
    * @param eventData 从mysql中读入的事件数据
    * @param time 当前时刻
    * @param dict 股票，行业，概念三类的完备字典
    * @param partitionNum 分区数
    * @note rowNum: 62
    */
  def save(jsonConfig: JsonConfig,
           sqlContext: SQLContext,
           mysqlConn: Connection,
           newsData: RDD[(Int, String, String, String)],
           eventData:RDD[(String, String)],
           time: String,
           dict: RDD[(String, String)],
           partitionNum: Int):Unit = {

    val date = TimeUtil.getDay(time)

    try {

      Array("stock", "industry", "section", "event").foreach(category => {

        val name = dict.filter(x => x._1 == category).map(x => (x._2, x._1))

        val finalResult = RelationCompute.getRelation(newsData, eventData)
          .filter(x => x._1.startsWith(category.substring(0,2)))
          .map(x => (x._1.split("_"), x._2 , x._3, x._4, x._5))
          .filter(x => x._1.length == 2)
          .map(x => (x._1(1), x._2, x._3, x._4, x._5))  //industry, section, stock, event
          .cache()

        if (category == "event") {

          val sql = s"insert into event_relation_history " +
            s"(date, re_stock, re_industry, re_section, re_event, event)" +
            s"VALUES" +
            s"(?,?,?,?,?,?)"
          val prepEvent = mysqlConn.prepareStatement(sql)

          finalResult.collect.foreach(row => {

            prepEvent.setString(1, date)
            prepEvent.setString(2, row._4)
            prepEvent.setString(3, row._2)
            prepEvent.setString(4, row._3)
            prepEvent.setString(5, row._5)
            prepEvent.setString(6, row._1)
            prepEvent.addBatch()

          })

          prepEvent.executeBatch()
          prepEvent.close()

        } else {

          val sql = s"insert into ${category}_relation_history" +
            s"(date, re_stock, re_industry, re_section, re_event, $category)" +
            s"VALUES" +
            s"(?,?,?,?,?,?)"
          val prepNews = mysqlConn.prepareStatement(sql)

          val result = finalResult.map(x => (x._1, (x._2, x._3, x._4, x._5)))
            .rightOuterJoin(name)
            .map(x => (x._1, x._2._1.getOrElse("", "", "", "")))
            .map(a => (a._1, a._2._1, a._2._2, a._2._3, a._2._4))

          result.collect.
            foreach(row => {

            prepNews.setString(1, date)
            prepNews.setString(2, row._4)
            prepNews.setString(3, row._2)
            prepNews.setString(4, row._3)
            prepNews.setString(5, row._5)
            prepNews.setString(6, row._1)
            prepNews.addBatch()

          })

          prepNews.executeBatch()
          prepNews.close()

        }

      })

    } catch {

      case e: Exception =>
        println(e.getLocalizedMessage)
        println(e)

    } finally {
      mysqlConn.close()
    }

  }

  /**
    * 调用以上方法，读入数据，计算，并保存
    *
    * @param jsonConfig 配置
    * @param sqlContext sql实例
    * @param partitionNum 分区数
    * @note rowNum: 56
    */
  def computeAndSaveToMysql(sc: SparkContext,
                            jsonConfig: JsonConfig,
                            sqlContext: SQLContext,
                            time: Long,
                            dict: RDD[(String, String)],
                            partitionNum: Int): Unit = {

    //获取数据
    val newsData = readNewsData(jsonConfig, sqlContext, time, partitionNum)
    val eventData = readEventData(jsonConfig,sqlContext, time, partitionNum)
    println(newsData.count() + "\t" + eventData.count())

    //配置mysql
    val mysqlUrl = jsonConfig.getValue("computeRelation", "jdbcUrlMatrix")
    val mysqlConn = RelationCompute.getConnect("com.mysql.jdbc.Driver", mysqlUrl)

    //将计算结果保存到mysql中
    save(jsonConfig,sqlContext,mysqlConn,newsData,eventData, time.toString, dict, partitionNum)


  }

}
