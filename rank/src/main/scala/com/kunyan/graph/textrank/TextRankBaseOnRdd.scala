package com.kunyan.graph.textrank

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcao on 2016/6/22.
  *
  * textrank算法主流程类:迭代计算评分部分为分布式RDD操作
  */
object TextRankBaseOnRdd {

  /**
    * 获取数组中某个索引区间的元素，形成一个子数组
    *
    * @param input 原始数组
    * @param start 开始的索引
    * @param end 结束的索引
    * @return 返回新的子数组
    * @note rowNum: 7
    */
  def arrayRange(input: Array[String], start: Int, end: Int): Array[String] = {

    val subArr = new ArrayBuffer[String]()

    for (i <- start to end) {
      subArr += input(i)
    }

    subArr.toArray
  }

  /**
    * 设置窗口，确定词与词之间的边关系
    *
    * @param text 文章分词后的array,每篇文章为数组的一个元素
    * @param size 窗口的大小
    * @return 如下图关系：
    *         (WORDi,Array(WORD_i1,WORD_i2,........WORD_in))
    *@note rowNum: 26
    */
  def buildRelation(text: Array[String], size: Int): Array[(String, Array[String])] = {

    var relation = ArrayBuffer[(String, Array[String])]()
    val arr = text
    val totalLen = arr.length

    for (i <- 0 until  totalLen ) {

      val headWord = i
      val backWord = totalLen - 1 - i
      val neighbor = new ArrayBuffer[String] ()

      if (headWord < size && backWord < size) {

        neighbor ++= arr.take(headWord)
        neighbor ++= arr.takeRight(backWord)

      } else if (headWord < size && backWord >= size) {

        neighbor ++= arr.take(headWord)
        neighbor ++= arrayRange(arr, headWord + 1, headWord + size)

      } else if (headWord >= size && backWord < size) {

        neighbor ++= arrayRange(arr, headWord - size, headWord - 1)
        neighbor ++= arr.takeRight(backWord)

      } else if (headWord >= size && backWord >= size) {

        neighbor ++= arrayRange(arr, headWord - size, headWord - 1)
        neighbor ++= arrayRange(arr, headWord + 1, headWord + size)

      }

      var a = (arr(i), neighbor.toArray)
      relation += a

      }

    relation.toArray
  }

  /**
    * 马尔可夫链循环迭代计算出每个词的评分
    *
    * @param sc SparkContext
    * @param data 每篇文章的图结构
    * @param iteration 迭代的次数
    * @return 每篇文章得分最高的前n个词
    * @note rowNum: 22
    */
  def computing (sc: SparkContext,
                 data:RDD[(String, Set[String])],
                 iteration: Int): Array[(String, Double)] = {

    var ranks = data.mapValues(v => 1.0 )

    for (i <- 1 to iteration) {

      val score = sc.accumulator(0.0)
      val contribute = data.join(ranks).values.flatMap {

        case (urls, rank) =>

          val size = urls.size

          if (size == 0) {

            score += rank
            List()

          } else {
            urls.map(url => (url, rank / size))
          }

      }

      ranks = contribute.reduceByKey(_ + _).mapValues[Double](p =>
        0.15 + 0.85 * p
      )

    }

    ranks.sortBy(word => word._2, ascending = false).collect
  }

  /**
    * 整合“确定边关系”与“迭代计算”两个方法
    *
    * @param sc sparkcontext
    * @param text 分词后的词项，也就是graph的vertex,Array的形式
    * @param windowSize 窗口大小
    * @param iteration 迭代次数
    * @return （词，得分）
    * @note rowNum: 10
    */
  def getTopHotWords(sc: SparkContext,
                     text: Array[String],
                     windowSize: Int,
                     iteration: Int): Array[(String, Double)] = {

    val wordGraph = buildRelation(text, windowSize)

    val rdd = sc.parallelize(wordGraph.map(x => (x._1, x._2)).toSeq)
      .groupByKey().map(line => (line._1, line._2.flatMap(x => x).toSet))

    computing(sc, rdd, iteration)
  }

}
