package com.kunyan

import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source

/**
  * Created by wangcao on 2016/06/17.
  */
object run {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("cosineCorr")
    val sc = new SparkContext(sparkConf)

    //设置参数
    val stopPuncPath = args(0)
    val stopWordsBr = sc.broadcast(Source.fromFile(stopPuncPath)
      .getLines().toArray).value
    val rawDataPath = args(1)
    val outputCosine = args(2)
    val para = args(3).split(",").map(_.toInt)

    //计算cosine值并保存到hdfs
    SegCosineCorrPastDay.computeCosine(sc, stopWordsBr, rawDataPath, outputCosine, para)

    sc.stop()
  }

}
