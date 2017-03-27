package com.kunyan.relation

import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.kunyan.util.TimeUtil

/**
  * Created by wangcao on 2017/2/10.
  *
  * 以天为单位运行所有历史关系图谱
  */
object RunHistory {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Warren_Relation").set("dfs.replication", "1").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取配置文件
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt

    //读入三类词典
    val dict = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\relation\\stablename\\dict_170213")
      .map(_.split("\t")).map(x => (x(0), x(1)))

    //时间数组
    val start = TimeUtil.getTimeStampDay("2017-01-01")
    val end = TimeUtil.getTimeStampDay("2017-02-01")
    val timeArr = List.range(start, end, 1000 * 60 * 60 * 24L)
    //timeArr.take(10).map(x => TimeUtil.getDay(x.toString)).foreach(println)

    timeArr.foreach(time => {

      //运行程序并保存最终结果到mysql
      RelationHistory.computeAndSaveToMysql(sc, jsonConfig, sqlContext, time, dict, partitionNum)

    })

    sc.stop()
  }


}
