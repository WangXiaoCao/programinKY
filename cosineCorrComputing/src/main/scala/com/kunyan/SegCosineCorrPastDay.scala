package com.kunyan

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern
import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix._
import com.kunyandata.nlpsuit.util.{TextPreprocessing, AnsjAnalyzer}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcao on 2016/6/17.
  * 根据搜索词计算cosine相关性
  */
object SegCosineCorrPastDay {

  /**
    * 提取时间
    *
    * @return 前一个小时的时间
    * @note rowNum: 5
    */
  def getPreHourStr: String = {

    val date = new Date(new Date().getTime - 60 * 60 * 1000)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-H")

    sdf.format(date)
  }

  /**
    * 转换字符
    *
    * @param code UTF-16编码
    * @return 转成UTF-8的编码
    * @author qq
    * @note rowNum: 17
    */
  def change8To16(code: String): String = {

    val url = code.split("%")
    val arr = ArrayBuffer[String]()

    if (!url(0).isEmpty) {
      arr += url(0)
    }

    for (i <- 1 until url.length) {

      if (!url(i).isEmpty && url(i).length == 5) {
        arr += ("%" + url(i).substring(1, 3))
        arr += ("%" + url(i).substring(3))
      } else if (!url(i).isEmpty && url(i).length == 2) {
        val tmp = URLDecoder.decode("%" + url(i), "UTF-8")
        arr += tmp
      }

    }

    arr.mkString("")
  }

  /**
    * url解码
    *
    * @param str url code
    * @return 解码后的文字
    * @author qq
    * @note rowNum: 22
    */
  def urlcodeProcess(str: String): String = {

    val findUtf8 = Pattern.compile("%([0-9a-fA-F]){2}").matcher(str).find
    val findUnicode = Pattern.compile("%u([0-9a-fA-F]){4}").matcher(str).find

    if (findUtf8 && !findUnicode) {

      try {
        urlcodeProcess(URLDecoder.decode(str, "UTF-8"))
      } catch {
        case e: Exception =>
          null
      }

    }
    else if (findUnicode) {

      try {
        urlcodeProcess(URLDecoder.decode(change8To16(str), "UTF-16"))
      } catch {
        case e: Exception =>
          null
      }

    }
    else
      str

  }

  /**
    * 将长度大于6的中文搜索词分词
    *
    * @param word 搜索词
    * @return 分词后的搜索词
    * @note rowNum: 7
    */
  def segWord(word: String): String = {

    var segSearchWord: String = word

    if (word.length >= 6 && !word.matches("[a-zA-Z.' ]+")) {
      segSearchWord = AnsjAnalyzer.cutNoTag(word).mkString(",")
    }

    segSearchWord
  }

  /**
    * 循环读取过去24小时的数据
    *
    * @param sc SparkContext
    * @param rawDataPath 读取数据的路径
    * @return 读取后的数据RDD
    * @note rowNum: 13
    */
  def readData(sc: SparkContext, rawDataPath: String): RDD[String] = {

    var unionInput: RDD[String] = sc.parallelize(Array(" "))

    for (i <- 1 to 24) {

      val date = new Date(new Date().getTime - 60 * 60 * 1000  * i)
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-H")
      val time = sdf.format(date)
      val day = time.substring(0, 10)
      val hour = time.substring(11)

      val input = sc.textFile(rawDataPath + day + "/" + hour)

      unionInput = unionInput.union(input)

    }

    unionInput
  }

  /**
    * 整合以上方法，计算cosine相似度
    *
    * @param sc SparkContext
    * @param stopWordsBr 停用词
    * @param rawDataPath 搜索词数据的路径
    * @param outputCosine 保存结果的路径
    * @param para (支持度，分区数，shuffle时的分区数)
    * @note rowNum: 28
    */
  def computeCosine(sc: SparkContext,
                    stopWordsBr: Array[String],
                    rawDataPath: String,
                    outputCosine: String,
                    para: Array[Int]): Unit = {

    //分词并计算cosine值
    val segSearchWord = readData(sc, rawDataPath).repartition(para(3))
      .map(_.split("\t"))
      .filter(x => x.length == 8)
      .filter(x => x(7) != "NoDef")
      .map(x => (x(1), urlcodeProcess(x(7))))
      .filter(x => x._2 != null)
      .map(x => (x._1, x._2.replace(" ", ",").replaceAll("[，。;；、]", ",")))
      .reduceByKey(_ + "," + _)
      .map(x => {

        val ip = x._1
        val words = x._2.split(",").map(a => segWord(a)).mkString(",")

        (ip, words)
      })
      .map(x => x._2.split(",").filter(a => a != ""))
      .filter(x => x.distinct.length <= 10000)
      .map(line => TextPreprocessing.removeStopWords(line, stopWordsBr))

    val result = computeCosineByRDD(sc, segSearchWord, para(0), para(1), para(2))

    //保存结果到上个小时的文本中
    val lastHourDate = getPreHourStr.substring(0, 10)
    val lastHour = getPreHourStr.substring(11)
    result.coalesce(1).saveAsTextFile(outputCosine + lastHourDate + "/" + lastHour)

  }

}
