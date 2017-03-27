package com.kunyan.util

import java.sql.{Connection, DriverManager}

/**
  * Created by zx on 2016/3/22.
  */
object MySQLUtil {

  /**
    * 建立连接
    *
    * @param driver 注册driver
    * @param jdbcUrl jdbcurl
    * @return 返回从数据库中读取的数据
    * @author LiYu
    * @note rowNum: 11
    */
  def getConnect(driver:String, jdbcUrl:String): Connection = {

    var connection: Connection = null

    try {

      // 注册Driver
      Class.forName(driver)
      // 得到连接
      connection = DriverManager.getConnection(jdbcUrl)
      // connection.close()

    }
    catch {
      case e:Exception => e.printStackTrace()
    }

    connection
  }

}
