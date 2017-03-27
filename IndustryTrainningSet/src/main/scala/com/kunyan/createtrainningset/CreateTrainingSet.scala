package com.kunyan.createtrainningset

import com.kunyandata.nlpsuit.util.AnsjAnalyzer
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by wangcao on 2017/2/22.
  *
  * 制作66个行业的二元平衡训练集
  */
object CreateTrainingSet {

  /**
    * 用ansj给文本分词
    *
    * @param text 一篇文本
    * @return  分词后的一篇文本
    */
  def segWord(text: String): Array[String] = {

    val pre = text.trim()

    val segSearchWord = AnsjAnalyzer.cutNoTag(pre).mkString("\t")
      .split("\t")
      .filter(x => x != "始##始")

    segSearchWord
  }


  /**
    * 为所有文本分词，去停
    *
    * @param stopWordsDir 停用词词典
    * @return 分词后的文本
    */
  def segWordAndRmStop(sc: SparkContext,
                       stopWordsDir: String,
                       inputPath: String,
                       outputPath: String,
                       cate: String): Unit= {

    // 获取某行业新闻文本
    val data = sc.textFile(inputPath + cate)

    //分词
    //AnsjAnalyzer.addUserDic("D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\dict\\user.txt", sc)
    val seg = data.map(x => segWord(x).filter(a => a != " "))
    //seg.map(x => x.mkString("/")).collect().foreach(println)

    //清洗
    val cleanSeg = seg.map(line => line.filter(word => word.matches("^[\u4E00-\u9FFF]+$") || word.matches("^\\d{6}$")))

    //去停
    val stopWords = Source.fromFile(stopWordsDir).getLines().toArray
    val titleSegRmst = cleanSeg.map(seg => seg.filter(x => !stopWords.contains(x)).mkString("/"))

    titleSegRmst.coalesce(1).saveAsTextFile(outputPath + cate)
    //titleSegRmst.collect().foreach(println)
  }

  def main(args: Array[String]):  Unit = {

    val conf = new SparkConf().setAppName("CreateTrainingSet").setMaster("local")
    val sc = new SparkContext(conf)

    val stopWordDir = "D:\\Documents\\cc\\smartdata项目资料\\情感分析优化\\dict\\stop_words_CN"
    val inputPath = "D:\\Documents\\cc\\smartdata项目资料\\分类优化\\行业分类\\行业分类\\行业文本\\"
    val outputPath = "D:\\Documents\\cc\\smartdata项目资料\\分类优化\\行业分类\\行业分类\\分词后的行业文本\\"
    //val cate = "白色家电"

    val indusArr = sc.textFile("D:\\Documents\\cc\\smartdata项目资料\\分类优化\\行业分类\\行业分类\\行业文本\\readme.txt")
        .map(x => x.trim())
        .map(_.split(",")(0))
        .map(x => x.replace("\uFEFF医药商业", "医药商业"))
        .collect()

    //indusArr.foreach(println)

    //1.分词并清洗文本
//    for (cate <- indusArr) {
//      segWordAndRmStop(sc, stopWordDir, inputPath, outputPath, cate)
//    }
//    segWordAndRmStop(sc, stopWordDir, inputPath, outputPath, "医药商业")

    //2.打上标签，所有文本合并成一个文件
    val labelOutput = "D:\\Documents\\cc\\smartdata项目资料\\分类优化\\行业分类\\行业分类\\打上标签的行业文本\\"

//    var totalNews = sc.parallelize(Array(("","")))
//
//    for (cate <- indusArr) {
//
//      val news = sc.textFile(outputPath + cate)
//        .map(x => (cate, x))
//
//      totalNews = totalNews.union(news)
//
//    }
//
//    val result = totalNews.filter(x => x._1.length > 1).map(x => x._1 + "," + x._2)
//
//    result.take(5).foreach(println)
//    result.coalesce(1).saveAsTextFile(labelOutput)

    // 3.制作每个行业的训练集
    val trainPath = "D:\\Documents\\cc\\smartdata项目资料\\分类优化\\行业分类\\行业分类\\各行业训练集\\"
    val summaryPath = "D:\\Documents\\cc\\smartdata项目资料\\分类优化\\行业分类\\行业分类\\各行业训练集信息\\"

    val totalNews = sc.textFile(labelOutput)
      .map(_.split(","))
      .filter(x => x.length == 2)
      .map(x => (x(0), x(1)))
      .cache()

    //val cate = "医药商业"

    val summary = new ArrayBuffer[String]

    for (cate <- indusArr) {

      val pos = totalNews.filter(x => x._1 == cate).map(x => 1 + "\t" + x._2)

      val totalLenght = 142936.toDouble
      val posLenght = pos.count().toDouble
      val negPercent = posLenght / (totalLenght - posLenght)
      val remPrecent = 1- negPercent

      val neg = totalNews.filter(x => x._1 != cate).randomSplit(Array(negPercent, remPrecent))(0).map(x => 0 + "\t" + x._2)

      val train = pos.union(neg)

      train.coalesce(1).saveAsTextFile(trainPath + cate)

      val summ = cate + "\t" + "正样本个数：" + posLenght + "\t" + "负样本个数：" + neg.count()
      summary += summ

    }

    sc.parallelize(summary).coalesce(1).saveAsTextFile(summaryPath)







  }

}
