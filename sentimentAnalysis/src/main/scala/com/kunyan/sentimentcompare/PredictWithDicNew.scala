package com.kunyan.sentimentcompare

import java.io.PrintWriter

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by zhangxin on 2017/3/1.
  * Modified by wangcao on 2017/3/3.
  *
  * 利用基于词性的方法进行情感分析
  *
  * 1、词性词典整理
  *
  * 2、测试语言：大盘开始回暖，风险依然激增
  */
object PredictWithDicNew {

  /**
    * 获取3类情感词，初始化Ansj 的词典
    *
    * @param dictPath 情感词典路径组成的数组
    * @return 正向情感词与词性标签的map，负向情感词与词性标签的map，否定词
    */
  def initDict(dictPath: Array[String]) = {

    val word_pos = Source.fromFile(dictPath(1)).getLines().toArray
      .map(_.replaceAll("\n", ""))
      .map(line => {

        val temp = line.split("/")
        val word = temp(0)
        val flag = temp(1)

        //添加到ansj中
        UserDefineLibrary.insertWord(word, flag, 1000)

        word -> flag
      }).toMap

    val word_neg = Source.fromFile(dictPath(2)).getLines().toArray
      .map(_.replaceAll("\n", ""))
      .map(line => {

        val temp = line.split("/")
        val word = temp(0)
        val flag = temp(1)

        //添加到ansj中
        UserDefineLibrary.insertWord(word, flag, 1000)

        word -> flag
      }).toMap

    // 否定词
    val word_fou = Source.fromFile(dictPath(3)).getLines().toArray

    (word_pos, word_neg, word_fou)
  }

  /**
    * 预测
    */
  def predict(word_pos: Map[String, String],
              word_neg: Map[String, String],
              word_fou: Array[String],
              text: String): Int = {

    // 为文本分词
    val re = NlpAnalysis.parse(text).toArray()
      .map(_.toString).reverse
      .filterNot(_.equals(" "))
      .filter(x => x.split("/").length == 2)

    val re_allwords = re.map(line => line.split("/")).filter(x => x.length == 2).map(x => x(0))

    // 初始化参数
    var sentiment = 0

    var rule_flag = false //如果触发规则，则置为True
    var last_flag = "" //上一个词的词性
    var last_sentiment_label = "" //上一个词的极性

    for (r <- re) {
      println(r)
      if (r.contains("/")) {
        val temp = r.split("/")
        val word = temp(0)
        val flag = temp(1)
        val index = re.indexOf(r)

        var flag_fou = false
        var sentiment_label = ""
        var temp_sentiment = 0

        // 正向
        if (word_pos.keySet.contains(word)) {
          sentiment_label = "pos"
        }

        // 负向
        if (word_neg.keySet.contains(word)) {
          sentiment_label = "neg"
        }

        // 否定词
        if (PredictWithDic.countSentiWithFou(index, re_allwords, word_fou) < 0) {
          flag_fou = true
        }

        // 规则一: neg_n + pos_v = neg
        // 规则二: neg_n + neg_v = pos
        // 规则三: pos_n + neg_n = neg  乐观行情现危机
        // 规则四: 标题两子句（前后情感不一，则取后句情感）

        if (!rule_flag) {
          if (last_flag == "" && last_sentiment_label == "" && sentiment_label == "pos") {
            last_flag = flag
            last_sentiment_label = sentiment_label
            temp_sentiment = 1
          } else if (last_flag == "" && last_sentiment_label == "" && sentiment_label == "neg") {
            last_flag = flag
            last_sentiment_label = sentiment_label
            temp_sentiment = -1
          } else if (last_flag == "" && last_sentiment_label == "" && sentiment_label != "") {
            last_flag = flag
            last_sentiment_label = sentiment_label
          } else if (last_flag.contains("v") && last_sentiment_label == "pos" && flag.contains("n") && sentiment_label == "neg") {
            temp_sentiment = -2
            rule_flag = true
          } else if (last_flag.contains("v") && last_sentiment_label == "neg" && flag.contains("n") && sentiment_label == "neg") {
            temp_sentiment = 1
            rule_flag = true
          } else if (last_flag.contains("n") && last_sentiment_label == "neg" && flag.contains("n") && sentiment_label == "pos") {
            temp_sentiment = -1
            rule_flag = true
          }
        }

        if (flag_fou) {
          sentiment += -temp_sentiment
        } else {
          sentiment += temp_sentiment
        }
      }

    }

    sentiment
  }

  /**
    * 整合初始化词典与预测两个步骤
    *
    * @param text     (标题，正文）
    * @param dictPath1 词典路径组成的数组
    * @return 一句话的情感值
    */
  def predictIntegration(sc: SparkContext,
                         text: (String, String),
                         dictPath1: Array[String],
                         dictPath2: Array[String],
                         titleOnly: Boolean): String = {

    val dicMap = PredictWithDic.initDict(sc, dictPath1)
    val title = text._1
    val content = text._2

    var senti = ""

    // 初始化词典
    val (word_pos, word_neg, word_fou) = initDict(dictPath2)

    // 预测标题的情感
    val predictTitle = predict(word_pos, word_neg, word_fou, title)

    if (predictTitle != 0) {
      if (predictTitle == 1) {
        senti = "pos"
      } else {
        senti = "neg"
      }
    } else {
      if (titleOnly) {
        senti = "null"
      } else {
        senti = PredictWithDic.predict(content, dicMap)
      }


    }
    senti
  }

}
