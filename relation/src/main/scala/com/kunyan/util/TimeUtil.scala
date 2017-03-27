package com.kunyan.util

import java.math.BigInteger
import java.text.SimpleDateFormat
import java.util.Locale

/**
  * Created by wangcao on 2016/08/13.
  * 时间转换的方法
  */
 object TimeUtil {

  /**
    * 将时间戳转换成yyyy-MM-dd格式
    *
    * @param timeStamp 时间戳
    * @return 年月日时
    * @note rowNum: 6
    */
  def getDay(timeStamp: String): String = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val bigInt: BigInteger = new BigInteger(timeStamp)
    val date: String = sdf.format(bigInt)

    date
  }

  /**
    * 将"yyyy-MM-dd"转换成时间戳（得到一天的0点0时0分）
    *
    * @param time 日期数据
    * @return 对应的时间戳
    * @note rowNum: 6
    */
  def getTimeStampDay(time: String): Long = {

    val loc = new Locale("en")
    val fm = new SimpleDateFormat("yyyy-MM-dd", loc)
    val dt2 = fm.parse(time)

    dt2.getTime()
  }

}
