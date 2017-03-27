package com.kunyan.sentimentpredict

import com.kunyandata.nlpsuit.sentiment.PredictWithDic
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangcao on 2017/3/10.
  *
  * 测试考虑进新闻标题有限判断的情感分析效果
  */
object Main {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Main").setMaster("local")
    val sc = new SparkContext(conf)

    val dict = Array(
      "D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\情感词典\\情感词典3.6\\user.txt",
      "D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\情感词典\\情感词典3.6\\pos.txt",
      "D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\情感词典\\情感词典3.6\\neg.txt",
      "D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\情感词典\\情感词典3.6\\fou.txt")

    val dictMap = PredictWithDic.init(sc, dict)

    val data = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\KUNYAN\\KUNYAN\\Dict\\鍒樼\uE757_scala\\test.txt")
      .map(x => (x, "0"))
      .map(text => {

        val result = PredictWithDic.predictWithTitle(text, dictMap, titleOnly=true)

        result + "\t" + text._1
      })

    data.collect().foreach(println)

    sc.stop()

  }

}
