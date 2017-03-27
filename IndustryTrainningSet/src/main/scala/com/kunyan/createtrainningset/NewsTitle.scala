package com.kunyan.createtrainningset

import java.util.{Properties, Date}

import com.kunyandata.nlpsuit.util.{AnsjAnalyzer, JsonConfig}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by wangcao on 2017/2/28.
  *
  * 统计所有新闻标题的词分布
  */
object NewsTitle {

  /**
    * 读取新闻id,平台号，url
    *
    * @param jsonConfig 配置文件
    * @param sqlContext sql实例
    * @return 新闻id,平台号，url,时间戳
    * @note rowNum: 19
    */
  def readNewsData(jsonConfig: JsonConfig,
                   sqlContext: SQLContext): RDD[(String)] = {

    val currentTimestamp = new Date().getTime

    val propNews = new Properties()
    propNews.setProperty("user", jsonConfig.getValue("computeRelation", "userNews"))
    propNews.setProperty("password", jsonConfig.getValue("computeRelation", "passwordNews"))
    propNews.setProperty("driver", "com.mysql.jdbc.Driver")

    sqlContext.read
      .jdbc(jsonConfig.getValue("computeRelation", "jdbcUrlNews"),
        "news_info", "news_time", 1L, currentTimestamp, 4, propNews)
      .registerTempTable("tempTableNews")

    val news = sqlContext.sql(s"select * from tempTableNews where type = 0")

    news.map(row => {
      (row.getString(4), row.getString(9))
    }).filter(x => x._2.length > 0)
      .map(x => x._1)

  }

  /**
    * 用ansj给文本分词并带上词性
    *
    * @param text 一篇文本
    * @return  分词后的文本
    */
  def segWord(text: String): Array[String] = {

    val pre = text.trim()

    val segSearchWord = AnsjAnalyzer.cutNoTag(pre).mkString("\t")
      .split("\t")
      .filter(x => x != "始##始")

    segSearchWord
  }

  def rmStopWords(seg: Array[String], stopWords: Array[String]): Array[String] = {

    seg.filter(x => !stopWords.contains(x))

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Warren_EventExposureAndVisit").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))

    //val title = readNewsData(jsonConfig, sqlContext)
    val title = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\data\\NewsTitle2")

    //分词
    AnsjAnalyzer.addUserDic("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\dict\\user.txt", sc)
    val titleSeg = title.map(x => segWord(x).filter(a => a != " "))

    //去停
    val stopWords = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\dict\\stop_words_CN").collect()
    val titleSegRmst = titleSeg.map(seg => seg.filter(x => !stopWords.contains(x)))

    //统计词频
    val statistic =titleSegRmst.flatMap(x => x).map(x => (x, 1)).reduceByKey(_+_)
      .map(x => (x._2, x._1)).sortByKey(ascending=false)
      .map(x => x._2 + "\t" + x._1)


    statistic.coalesce(1).saveAsTextFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\data\\NewsTitleStatistic3")//take(10).foreach(println)
    //titleSeg.coalesce(1).saveAsTextFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\data\\NewTitleSeg2")//take(10).foreach(println)
    //title.coalesce(1).saveAsTextFile("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\data\\NewsTitle2")
   // title.take(10).foreach(println)

  }

}
