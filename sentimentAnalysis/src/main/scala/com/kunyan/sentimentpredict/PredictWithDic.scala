package com.kunyan.sentimentpredict

import com.kunyandata.nlpsuit.util.AnsjAnalyzer
import org.apache.spark.SparkContext

/**
  * Created by zhangxin on 2016/7/19.
  * Modified by wangcao on 2017/3/01.
  *
  * 基于极性词典的方法进行情感分析预测
  * 对外提供两个方法
  * 1、init 初始化（加载用户词典和情感词典）
  * 2、predict 情感分析预测
  *
  * 注意：短文本和无明显情感值的文本统一返回值为Null
  */
object PredictWithDic {

  /**
    * 初始化： 添加用户词典，并获取情感词典
    * @param sc spark
    * @param dictPath 词典路径数组
    * @return 情感词典Map
    * @author zhangxin
    * @note 7
    *
    * 注意：dicPath的Array中词典顺序(user_dic, pos_dic, neg_dic.txt, fou_dic)
    */
  def init(sc: SparkContext, dictPath: Array[String]): Map[String, Array[String]] = {

    // ansj添加用户词典
    AnsjAnalyzer.init(sc, dictPath)

    // 读取词典
    val posDic = sc.textFile(dictPath(1)).collect()
    val negDic = sc.textFile(dictPath(2)).collect()
    val fouDic = sc.textFile(dictPath(3)).collect()

    // 构造情感词典Map,并返回
    Map("Pos" -> posDic, "Neg" -> negDic, "Fou" -> fouDic)
  }

  /**
    * 情感分析预测
    * @param content 文章内容
    * @param dicMap 情感词典
    * @return 情感倾向值：pos neg null
    * @author zhangxin
    * @note 24
    */
  def predict(content: String, dicMap: Map[String, Array[String]]): String= {

    //过滤超短文章
    //if(content.length < 200) return null

    // 记录正面负面倾向的次数
    var positive = 0
    var negative = 0

    //文章切分为句子
    val sentenceArr = content.split(",|。|\t|\n|，|：")

    sentenceArr.foreach(sentence => {

      val sentimentTemp = countSenti(sentence, dicMap)

      sentimentTemp match {
        case 1 => positive += 1
        case -1 => negative += 1
        case _ =>  //不做任何操作
      }

    })

    // 文章情感值
    val sentiment = positive-negative

    // 设定阈值并返回结果
    if(positive > 1 && negative > 1 && sentiment == 0){
      "pos"
    }else  if(positive >= 2 && sentiment > 0){
      "pos"
    }else if(negative >= 2 && sentiment < 0){
      "neg"
    }else{
      null
    }
  }

  /**
    * 对长篇文章以句子为单位计算其情感值
    *
    * @param sentence 句子
    * @param dicMap 情感词典map
    * @return 句子的情感值
    * @author zhangxin
    * @note 25
    */
  private def countSenti(sentence: String, dicMap: Map[String, Array[String]]): Int = {

    // 情感值
    var posCount = 0
    var negCount = 0

    // 句子切词
    val sentenceSeg = AnsjAnalyzer.cutNoTag(sentence)

    // 对分词后的每一个词匹配词典
    for (i <- sentenceSeg.indices) {

      val word = sentenceSeg(i)

      // pos
      if(dicMap("Pos").contains(word)){

        if(countSentiWithFou(i, sentenceSeg, dicMap("Fou"))>0){
          posCount += 1
        } else {
          negCount += 1
        }

      }

      // neg
      if (dicMap("Neg").contains(word)){

        if(countSentiWithFou(i, sentenceSeg, dicMap("Fou")) > 0){
          negCount += 1
        } else {
          posCount += 1
        }

      }

    }

    if(posCount > negCount) return 1
    if(posCount < negCount) return -1

    0
  }

  /**
    * 否定词对情感值的翻转作用
    *
    * @param i 当前情感词在句子中的位置
    * @param sentence 当前待分析的句子
    * @param dictionary 否定词词典
    * @return 返回（+1表示不翻转，-1表示翻转）
    * @author liumiao
    * @note 22
    */
  def countSentiWithFou(i: Int, sentence: Array[String], dictionary: Array[String]): Int = {

    // 寻找情感词前面的否定词，若有则返回-1
    if (i-1 > 0){

      if (dictionary.contains(sentence(i-1))){
        return -1
      } else if (i-2 > 0){
        if (dictionary.contains(sentence(i-2))){
          return  -1
        }
      }

    }

    // 寻找情感词后面的否定词，若有则返回-1
    if (i+1 < sentence.length){

      if(dictionary.contains(sentence(i+1))){
        return -1
      } else if (i+2 < sentence.length){
        if (dictionary.contains(sentence(i+2))){
          return -1
        }
      }

    }

    // 匹配不到否定词，则返回1
    1
  }

  /**
    * 先预测标题情感，若标题无情感则预测正文情感
    *
    * @param text 新闻（标题，正文）
    * @param dicMap 4个词典
    * @return 该篇新闻的情感：neg/pos/null
    * @author wangcao
    */
  def predictWithTitle(text: (String, String),
                       dicMap: Map[String, Array[String]],
                       titleOnly: Boolean): String = {

    val title = text._1
    val content = text._2

    var senti = ""

    val predictTitle = countSenti(title, dicMap)

    if (predictTitle != 0) {
      if (predictTitle == 1) {
        senti = "pos"
      } else {
        senti = "neg"
      }
    } else {
      if (titleOnly) {
        senti = null

      } else {
        senti = predict(content, dicMap)
      }

    }

    senti
  }

}
