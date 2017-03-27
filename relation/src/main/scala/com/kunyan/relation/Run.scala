package com.kunyan.relation

import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/07/18.
  *
  * 每半小时运行一次前24小时的关系图谱
  */
object Run {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Warren_Relation").set("dfs.replication", "1")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取配置文件
    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt

    //运行程序并保存最终结果到mysql
    RelationCompute.computeAndSaveToMysql(sc, jsonConfig, sqlContext, partitionNum)

    sc.stop()
  }

}
