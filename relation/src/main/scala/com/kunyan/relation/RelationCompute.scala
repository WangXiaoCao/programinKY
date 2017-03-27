package com.kunyan.relation

import java.sql.{DriverManager, Connection}
import java.util.{Date, Properties}
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by wangcao on 2016/7/18.
  *
  * 计算股票，行业，概念，事件各自最相关的股票，行业，概念，事件
  */
object RelationCompute {

  /**
    * 读取新闻及其属性的数据
    *
    * @param jsonConfig 配置文件
    * @return 新闻id, 及其股票，概念，行业的三类属性
    * @note rowNum: 21
    */
  def readNewsData(jsonConfig: JsonConfig,
                   sqlContext: SQLContext,
                   partitionNum: Int): RDD[(Int, String, String, String)] = {

    val time = jsonConfig.getValue("computeRelation", "newsTimeWindow").toLong
    val currentTimestamp = new Date().getTime
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
    * @return 事件名称与其相关新闻id
    * @note rowNum: 23
    */
  def readEventData(jsonConfig: JsonConfig,
                    sqlContext: SQLContext,
                    partitionNum: Int): RDD[(String, String)] = {

    val time = jsonConfig.getValue("computeRelation", "eventTimeWindow").toLong
    val currentTimestamp = new Date().getTime
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
      s"where end_time > $lastTimeStamp")

    event.map(row => {
      (row.getString(1), row.getString(3), row.getInt(11))
    })
      .repartition(partitionNum)
      .filter(x => x._2.length > 0)
      .filter(x => x._3 == 1)
      .map(x => (x._1, x._2))

  }

  /**
    * 将输入数据转变为以property为key， Array[newsId]为value的格式
    *
    * @param property 新闻的三种属性
    * @param eventTotal 事件名称与对应的新闻id
    * @return  (property, Array[news_id])
    * @note rowNum: 13
    */
  def propertyAsKey(property: RDD[(Int, String, String, String)],
                    eventTotal: RDD[(String, String)]): RDD[(String, Array[Int])] = {

    val flatProperty = property.map(line => {

      val industry = line._2.split(",").map(x => ("in_" + x, line._1))
      val section = line._3.split(",").map(x => ("se_" + x, line._1))
      val stock = line._4.split(",").map(x => ("st_" + x, line._1))

      industry.union(section).union(stock)
    })
      .flatMap(x => x)
      .groupByKey()
      .map(x => (x._1, x._2.toArray))

    val flatEvent = eventTotal.map(x => ("ev_" + x._1, x._2.split(",").map(_.toInt)))

    val total = flatProperty.union(flatEvent)

    total
  }

  /**
    * 将输入数据转变成以news_id为key的数据格式
    *
    * @param property 新闻的三种属性
    * @param eventTotal 事件名称与对应的新闻id
    * @return (news_id, Array[property])即返回新闻已经与该新闻相关的属性集合
    * @note rowNum: 14
    */
  def newsAsKey(property: RDD[(Int, String, String, String)],
                eventTotal: RDD[(String, String)]): Map[Int, Array[String]] = {

    val flatNews = property.map(line => {

      val industry = line._2.split(",").map(x => (line._1, "in_" + x))
      val section = line._3.split(",").map(x => (line._1, "se_" + x))
      val stock = line._4.split(",").map(x => (line._1, "st_" + x))

      industry.union(section).union(stock)
    }).flatMap(x => x)

    val flatEventEvent = eventTotal.map(line => line._2.split(",").map(x => (x.toInt, "ev_" + line._1)))
      .flatMap(x => x)

    val total = flatNews.union(flatEventEvent).groupByKey().map(x => (x._1, x._2.toArray))
      .collect().toMap

    total
  }

  /**
    * 选择最相关的前n个
    *
    * @param pro 属性与新闻id的集合
    * @param cate 属性的名称
    * @param rm 属性本身
    * @return 前15个最相关
    * @note rowNum: 10
    */
  def selectTop(pro: Array[(String, Int)], cate: String, rm: String): String = {

    pro.filter(x => x._1.startsWith(cate))
      .filter(x => x._1 != rm)
      .filter(x => x._2 > 0)
      .sortWith(_._2 > _._2)
      .map(_._1.split("_"))
      .filter(x => x.length == 2)
      .map(x => x(1))
      .take(15).mkString(",")

  }

  /**
    * 获取股票，行业，概念，事件各自最相关的股票，行业，概念，事件
    *
    * @param property 新闻的三种属性
    * @param eventTotal 事件名称与对应的新闻id
    * @return (对象，最相关的行业，最相关的概念，最相关的股票，最相关的事件)
    * @note rowNum: 17
    */
  def getRelation(property: RDD[(Int, String, String, String)],
                  eventTotal: RDD[(String, String)]): RDD[(String, String, String, String, String)]= {

    val news = newsAsKey(property, eventTotal)

    val process = propertyAsKey(property, eventTotal)
      .map(line => {

        val pro = line._2.map(x => news(x)).flatMap(a => a)
          .map(a => (a, 1))
          .groupBy(_._1)
          .map(line => (line._1, line._2.map(_._2).sum)).toArray

        val industry = selectTop(pro, "in_", line._1)
        val section = selectTop(pro, "se_", line._1)
        val stock = selectTop(pro, "st_", line._1)
        val event = selectTop(pro, "ev_", line._1)

        (line._1, industry, section, stock, event)
      })

    process
  }

  /**
    * 建立连接
    *
    * @param driver  注册driver
    * @param jdbcUrl jdbcurl
    * @return 返回从数据库中读取的数据
    * @author LiYu
    * @note rowNum: 11
    */
  def getConnect(driver: String, jdbcUrl: String): Connection = {

    var connection: Connection = null

    try {

      Class.forName(driver)
      connection = DriverManager.getConnection(jdbcUrl)

    }
    catch {
      case e: Exception => e.printStackTrace()
    }

    connection
  }


  /**
    * 保存计算结果到mysql中
    *
    * @param jsonConfig 配置文件
    * @param sqlContext sql实例
    * @param mysqlConn sql连接
    * @param newsData 从mysql中读入的新闻数据
    * @param eventData 从mysql中读入的事件数据
    * @param partitionNum 分区数
    * @note rowNum: 62
    */
  def save(jsonConfig: JsonConfig,
           sqlContext: SQLContext,
           mysqlConn: Connection,
           newsData: RDD[(Int, String, String, String)],
           eventData:RDD[(String, String)],
           partitionNum: Int):Unit = {

    try {

      val sqlDelete = "delete from event_relation"
      val prep = mysqlConn.prepareStatement(sqlDelete)
      prep.executeUpdate()
      prep.close()

    } catch {

      case e: Exception =>
        println(e.getLocalizedMessage)
        println(e)

    }

    try {

      val result = getRelation(newsData, eventData).cache()

      Array("stock", "industry", "section", "event").foreach(category => {

        val finalResult = result
          .filter(x => x._1.startsWith(category.substring(0,2)))
          .map(x => (x._1.split("_"), x._2 , x._3, x._4, x._5))
          .filter(x => x._1.length == 2)
          .map(x => (x._1(1), x._2, x._3, x._4, x._5))
          .collect()

        if (category == "event") {

          val sql = s"insert into event_relation " +
            s"(re_stock, re_industry, re_section, re_event, event)" +
            s"VALUES" +
            s"(?,?,?,?,?)"
          val prepEvent = mysqlConn.prepareStatement(sql)

          finalResult.foreach(row => {

            prepEvent.setString(1, row._4)
            prepEvent.setString(2, row._2)
            prepEvent.setString(3, row._3)
            prepEvent.setString(4, row._5)
            prepEvent.setString(5, row._1)
            prepEvent.addBatch()

          })

          prepEvent.executeBatch()
          prepEvent.close()

        } else {

          val sql = s"update ${category}_relation set re_stock = ?, " +
            s"re_industry = ?, re_section = ?, re_event = ? where $category = ?"
          val prepNews = mysqlConn.prepareStatement(sql)

          finalResult.foreach(row => {

            prepNews.setString(1, row._4)
            prepNews.setString(2, row._2)
            prepNews.setString(3, row._3)
            prepNews.setString(4, row._5)
            prepNews.setString(5, row._1)
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
    * @note rowNum: 9
    */
  def computeAndSaveToMysql(sc: SparkContext,
                            jsonConfig: JsonConfig,
                            sqlContext: SQLContext,
                            partitionNum: Int): Unit = {

    //获取数据
    val newsData = readNewsData(jsonConfig, sqlContext, partitionNum)
    val eventData = readEventData(jsonConfig,sqlContext, partitionNum)

    //配置mysql
    val mysqlUrl = jsonConfig.getValue("computeRelation", "jdbcUrlNews")
    val mysqlConn = getConnect("com.mysql.jdbc.Driver", mysqlUrl)

    //将计算结果保存到mysql中，对于事件表每次保存前先删除掉上一次保存的表内容
    save(jsonConfig,sqlContext,mysqlConn,newsData,eventData,partitionNum)

  }

}
