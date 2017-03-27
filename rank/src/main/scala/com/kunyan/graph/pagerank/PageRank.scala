package com.kunyan.graph.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 *Created by Wangcao on 3/9/16.
 *
 *计算每个url的评分，根据评分筛选出最优的链接
 */
  
object PageRank {

  /**
    * 创建关系图
    *
    * @param sc SparkContext
    * @param lines 读入的数据,Array形式（vertex, neighbor vertex)
    * @return (vertex, (neighbor1，neighbor2...)
    * @note rowNum: 22
    */
  def buildGraph(sc: SparkContext, lines: RDD[Array[String]]): RDD[(String, Iterable[String])] = {

    val links = lines.filter(_.length == 2)
      .map(parts => (parts(0), parts(1))).distinct().groupByKey()

    val nodes = scala.collection.mutable.ArrayBuffer.empty ++ links.keys.collect()

    val newNodes = scala.collection.mutable.ArrayBuffer[String]()

    for {s <- links.values.collect()
         k <- s if ! nodes.contains(k)
    } {
      nodes += k
      newNodes += k
    }

    links ++ sc.parallelize(for (i <- newNodes) yield (i, List.empty))
  }

  /**
    * 迭代计算得分
    *
    * @param sc SparkContext
    * @param linkList 点与点之间的图关系
    * @param iteration 迭代的次数
    * @return 每个点及其得分降序排列
    * @note rowNum: 20
    */
  def computing(sc: SparkContext, linkList: RDD[(String, Iterable[String])], iteration: Int): RDD[(String, Double)] = {

    var ranks = linkList.mapValues(v => 1.0 )

    for (i <- 1 to iteration) {

      val score = sc.accumulator(0.0)

      val contribute = linkList.join(ranks).values.flatMap {

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

    ranks.sortBy(word => word._2, ascending = false)
  }

  /**
    * 整合“构建图” 与 “迭代计算评分”两个方法
    *
    * @param sc SparkContext
    * @param lines 读入的数据,Array形式（vertex, neighbor vertex)
    * @param iteration 迭代的次数
    * @return 每个点，及其评分，降序排列
    * @note rowNum: 6
    */
  def runPageRank(sc: SparkContext, lines: RDD[Array[String]], iteration: Int): RDD[(String, Double)] = {

    val linkList = buildGraph(sc, lines)

    val rankedOutput = computing(sc, linkList, iteration)

    rankedOutput
  }

}  