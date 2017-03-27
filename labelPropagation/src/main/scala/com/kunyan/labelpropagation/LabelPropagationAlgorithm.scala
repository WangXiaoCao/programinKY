package com.kunyan.labelpropagation

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcao on 2016/5/17.
  *
  * 标签传播算法主流程
  */
object LabelPropagationAlgorithm {

  /**
    * 传播概率与标签矩阵相乘
    *
    * @param W 传播概率
    * @param Y 标签矩阵
    * @return 新的标签矩阵
    * @note rowNum: 21
    */
  def multiplyMatrix(W: RDD[((Long, Long), Double)], Y: RDD[((Long, Int, Int), (Double, Double))]) = {

    val tempW = W.map(x => (x._1._2, (x._1._1, x._2)))
    val tempY = Y.map(x => (x._1._1, (x._1._2, x._2)))
    val matrixTemp = tempW.join(tempY).map(x => ((x._2._1._1, x._1),
      (x._2._1._2 * x._2._2._2._1, x._2._1._2 * x._2._2._2._2, x._2._2._1)))

    val tempResult = matrixTemp.map(x => (x._1._1, (x._2._1, x._2._2)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    val resultLabel = tempResult.join(Y.map(x => (x._1._1, (x._1._2, x._1._3)))).map(x => {

      val total = x._2._1._1 + x._2._1._2

      if (x._2._2._1 == 1) {

        if (x._2._2._2 == 1) {
          ((x._1, x._2._2._1, x._2._2._2), (0.0, 1.0))
        } else {
          ((x._1, x._2._2._1, x._2._2._2), (1.0, 0.0))
        }

      } else {
        ((x._1, x._2._2._1, x._2._2._2), (x._2._1._1 / total, x._2._1._2 / total))
      }

    })

    resultLabel
  }

  /**
    * 主方法
    * @note rowNum: 53
    */
  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LabelPropagationAlgorithm").setMaster("local")
    val sc = new SparkContext(conf)

    val ITERATION = 50

    //1. 创建顶点(id, news, knownLabel, label)
    val labelPoint = sc.parallelize(Array((1,123,1,1),(2,124,0,1),(3,125,0,0),(4,126,1,0),(5,127,0,0)))
      .map(x => (x._1.toLong, x._2.toString, x._3.toInt, x._4.toInt))

    val newsVertex: RDD[(VertexId, String)] = labelPoint.map(x => (x._1, x._2))


    //2. 创建边
    val similarity = sc.parallelize(Array((1,2,0.8),(1,3,0.7),(1,4,0.2),(1,5,0.1),
      (2,3,0.9),(2,4,0.1),(2,5,0.05),(3,4,0.1),(3,5,0.1),(4,5,0.99)))

    val totalSimilarityTemp = similarity.union(similarity.map(x => (x._2, x._1, x._3)))

    val totalSimilarity = totalSimilarityTemp.union(totalSimilarityTemp.map(x => (x._1, x._1, 1.0)).distinct)

    val edgeRecord: RDD[Edge[Double]] = totalSimilarity.map(x => Edge(x._1, x._2, x._3.toDouble))


    //3. 创建图
    val graph = Graph(newsVertex, edgeRecord)


    //4. 创建标签矩阵
    val labelMatrixTemp = labelPoint.map(x => ((x._1, x._3, x._4),(0.0, 0.0)))

    val labelMatrix = labelMatrixTemp.map{case(k,v) =>

      if (k._3 == 1) {
        (k, (0.0, 1.0))
      } else {
        (k, (1.0, 0.0))
      }

    }


    //5. 创建传播概率（边的权重）
    //计算相似度
    val weight = graph.triplets.map(x => ((x.srcId.toLong, x.dstId.toLong), x.attr))

    //行的归一化
    val rowSum = weight.map(x => (x._1._1, x._2)).reduceByKey(_+_)
    val propagationPro = weight.map(x => (x._1._1, (x._1._2, x._2))).join(rowSum)
      .map(x => ((x._1,x._2._1._1),x._2._1._2 / x._2._2))


    //6.新标签矩阵 = 传播概率*标签矩阵
    var updateLabelMatrix = multiplyMatrix(propagationPro, labelMatrix)

    //提取出前一次每个顶点及其标签
    var labelRecordBefore = updateLabelMatrix.map(l => (l._1._1, l._2._2))

    var recordDifArray = ArrayBuffer[Double]()

    //循环迭代
    for (i <- 1 to ITERATION) {

      updateLabelMatrix = multiplyMatrix(propagationPro, updateLabelMatrix)

      val labelRecordAfter = updateLabelMatrix.map(x => (x._1._1, x._2._2))

      val labelRecordDifference = labelRecordAfter.union(labelRecordBefore)
        .reduceByKey(_ - _)
        .map(l => (1, Math.abs(l._2)))
        .reduceByKey(_ + _)

      labelRecordBefore = labelRecordAfter

      recordDifArray += labelRecordDifference.values.first()

    }

    updateLabelMatrix.coalesce(1).saveAsTextFile(args(0))


    //7. 最后标签结果
    val finalLabel = updateLabelMatrix.map{case(k, v) =>

      if (v._1 >= 0.5) {
        (k, 0.0)
      } else {
        (k, 1.0)
      }

    }

    finalLabel.coalesce(1).saveAsTextFile(args(1))


    //8.保存每次迭代的差异和
    val recordDif = sc.parallelize(recordDifArray.toSeq)

    val recordDifMap = recordDif.map(w => w)

    recordDifMap.coalesce(1).saveAsTextFile(args(2))

    sc.stop()

  }

}
