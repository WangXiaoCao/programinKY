package com.kunyan.sentimentdic

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2017/3/16.
  *
  * 整理文本情感词典
  */
object addDic {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("addDic").setMaster("local")
    val sc = new SparkContext(conf)

//    val negOrig = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\sentiment_dicts\\sentiment_dicts3.10\\neg.txt")
//
//    val data = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\w2v情感词\\part-00000")
//      .map(_.split(": "))
//      .filter(x => x.length == 2)
//      .map(x => x(1).trim())
//      .map(_.split(" "))
//      .flatMap(x => x)
//      .union(negOrig)
//      .distinct()
//
//    data.coalesce(1).saveAsTextFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\sentiment_dicts\\sentiment_dicts3.16\\neg.txt")


    val posOrig = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\sentiment_dicts\\sentiment_dicts3.10\\pos.txt")

    val data = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\w2v情感词\\edit.txt")
      .map(x => x.replace("\uFEFF超", "超"))
      .map(x => x.trim())
      .map(_.split(" "))
      .flatMap(x => x)
      .union(posOrig)
      .distinct()

    //data.take(10).foreach(println)
    data.coalesce(1).saveAsTextFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\sentiment_dicts\\sentiment_dicts3.16\\pos")

  }

}
