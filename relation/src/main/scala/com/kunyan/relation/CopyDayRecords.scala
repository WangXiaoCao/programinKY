package com.kunyan.relation

import java.sql.Connection
import java.util.{Date, Properties}
import com.kunyan.util.TimeUtil
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by wangcao on 2017/2/20.
  *
  * 每天23点58分将当前的实时关系图谱添加到累计关系图谱的表中
  */
object CopyDayRecords {

  /**
    * 去除null值
    *
    * @param tem 字段内容
    * @return 非null的原值返回，Null值转换成""
    * @note rowNum: 7
    */
  def rmNull(tem: String): String = {
    val tem2 = if (tem == null) {
      ""
    } else {
      tem
    }

    tem2
  }

  /**
    * 从实时更新的关系图谱表中读取当前的关联图谱
    *
    * @param jsonConfig 配置文件
    * @return 关联图谱数据
    * @note rowNum: 18
    */
  def readRelationData(jsonConfig: JsonConfig,
                       sqlContext: SQLContext,
                       tableName: String): RDD[(String, String, String, String, String)] = {

    val propRelation = new Properties()

    propRelation.setProperty("user", jsonConfig.getValue("computeRelation", "userNews"))
    propRelation.setProperty("password", jsonConfig.getValue("computeRelation", "passwordNews"))
    propRelation.setProperty("driver", "com.mysql.jdbc.Driver")

    sqlContext.read
      .jdbc(jsonConfig.getValue("computeRelation", "jdbcUrlNews"),
        s"$tableName", propRelation)
      .registerTempTable("tempTableNews")

    val event = sqlContext.sql(s"select * from tempTableNews")

    event.map(row => {
      (row.getString(0), row.getString(1), row.getString(2),
        row.getString(3), row.getString(4))
    })
      .map(x => (rmNull(x._1), rmNull(x._2), rmNull(x._3), rmNull(x._4), rmNull(x._5)))

  }

  /**
    * 保存结果到mysql中
    *
    * @param mysqlConn Connection
    * @param relationData 关系图谱数据
    * @param newTableName 待存入的表的名称
    * @param date 日期
    * @note rowNum: 22
    */
  def save(mysqlConn: Connection,
           relationData:RDD[(String, String, String, String, String)],
           newTableName: String,
           date: String) = {

    val name = newTableName.split("_")(0)
    val sql = s"insert into $newTableName " +
      s"(date, re_stock, re_industry, re_section, re_event, $name)" +
      s"VALUES" +
      s"(?,?,?,?,?,?)"
    val prepEvent = mysqlConn.prepareStatement(sql)

    relationData.collect.foreach(row => {

      prepEvent.setString(1, date)
      prepEvent.setString(2, row._2)
      prepEvent.setString(3, row._3)
      prepEvent.setString(4, row._4)
      prepEvent.setString(5, row._5)
      prepEvent.setString(6, row._1)
      prepEvent.addBatch()

    })

    prepEvent.executeBatch()
    prepEvent.close()

  }

  /**
    * 整合以上过程，将当前实时更新的关系图谱表的数据存到以天累计的关系图谱表中
    *
    * @param jsonConfig 配置文件
    * @param sqlContext sql实例
    * @note rowNum: 22
    */
  def integration(jsonConfig: JsonConfig,
           sqlContext: SQLContext):Unit = {

    val date = TimeUtil.getDay(new Date().getTime.toString)

    val event = readRelationData(jsonConfig, sqlContext, "event_relation")
    val stock = readRelationData(jsonConfig, sqlContext, "stock_relation")
    val industry = readRelationData(jsonConfig, sqlContext, "industry_relation")
    val section = readRelationData(jsonConfig, sqlContext, "section_relation")

    //配置mysql
    val mysqlUrl = jsonConfig.getValue("computeRelation", "jdbcUrlNews")
    val mysqlConn = RelationCompute.getConnect("com.mysql.jdbc.Driver", mysqlUrl)

    try {

      save(mysqlConn, event, "event_relation_history", date)
      save(mysqlConn, stock, "stock_relation_history", date)
      save(mysqlConn, industry, "industry_relation_history", date)
      save(mysqlConn, section, "section_relation_history", date)

    } catch {

      case e: Exception =>
        println(e.getLocalizedMessage)
        println(e)

    } finally {
      mysqlConn.close()
    }

  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Warren_CopyDayRecords").set("dfs.replication", "1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取配置文件
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))

    //运行程序并保存最终结果到mysql
    integration(jsonConfig, sqlContext)

    sc.stop()
  }

}
