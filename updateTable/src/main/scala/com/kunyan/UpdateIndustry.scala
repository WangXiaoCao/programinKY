package com.kunyan

import java.sql.{DriverManager, Connection}
import java.util.{Properties, Date}
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by wangcao on 2017/2/13.
  *
  * 将msql数据库中的一张表的信息更新到另一张表中
  */
object UpdateIndustry {

  def readBoard(jsonConfig: JsonConfig,
                sqlContext: SQLContext): RDD[(String, String, String)] = {

    val currentTimestamp = new Date().getTime

    val propNews = new Properties()
    propNews.setProperty("user", jsonConfig.getValue("computeRelation", "userStock"))
    propNews.setProperty("password", jsonConfig.getValue("computeRelation", "passwordStock"))
    propNews.setProperty("driver", "com.mysql.jdbc.Driver")

    sqlContext.read
      .jdbc(jsonConfig.getValue("computeRelation", "jdbcUrlStock"),
        "SH_SZ_BOARDMAP",propNews)
      .registerTempTable("tempTableNews")

    val board = sqlContext.sql(s"select SYMBOL, BOARDCODE, KEYNAME from tempTableNews")

    board.map(row => {
      (row.getString(0), row.getString(1), row.getString(2))
    })

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

  def update(jsonConfig: JsonConfig,
             sqlContext: SQLContext,
             mysqlConn: Connection,
             tableName: String,
             data: RDD[String]) = {

    val name = tableName.split("_")(0)

    try {

      val sqlDelete = s"delete from $tableName"
      val prep = mysqlConn.prepareStatement(sqlDelete)
      prep.executeUpdate()
      prep.close()

    } catch {

      case e: Exception =>
        println(e.getLocalizedMessage)
        println(e)

    }

    try {

      val sql = s"insert into $tableName " +
        s"($name)" +
        s"VALUES" +
        s"(?)"
      val prepEvent = mysqlConn.prepareStatement(sql)

      data.collect().foreach(row => {

        prepEvent.setString(1, row)
        prepEvent.addBatch()

      })

      prepEvent.executeBatch()
      prepEvent.close()

    } catch {

      case e: Exception =>
        println(e.getLocalizedMessage)
        println(e)

    } finally {
      mysqlConn.close()
    }

  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("updateIndustry").set("dfs.replication", "1").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取配置文件
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))

    val board = readBoard(jsonConfig, sqlContext).cache()
   // board.take(10).foreach(println)

    val stock  = board.map(x => x._1).distinct().map(x => "stock" + "\t" + x)
    val section = board.filter(x => x._2 == "1105").map(x => x._3).distinct().map(x => "section" + "\t" + x)
    val industry = board.filter(x => x._2 == "1109").map(x => x._3).distinct().map(x => "industry" + "\t" + x)

    //配置mysql
//    val mysqlUrl = jsonConfig.getValue("computeRelation", "jdbcUrlNews")
//    val mysqlConn = getConnect("com.mysql.jdbc.Driver", mysqlUrl)

   // update(jsonConfig, sqlContext, mysqlConn, "stock_relation", stock)
    //update(jsonConfig, sqlContext, mysqlConn, "section_relation", section)
   // update(jsonConfig, sqlContext, mysqlConn, "industry_relation", industry)

    val data = stock.union(section).union(industry)

    data.coalesce(1).saveAsTextFile("D:\\Documents\\cc\\smartdata项目资料\\relation\\stablename\\dict2")

    sc.stop()
  }

}
