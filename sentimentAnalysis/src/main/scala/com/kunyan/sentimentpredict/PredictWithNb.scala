package com.kunyan.sentimentpredict

import java.io._
import java.math.{BigDecimal, RoundingMode}
import java.text.DecimalFormat

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.{ChiSqSelectorModel, HashingTF, IDFModel}

/**
  * Created by zhangxin on 2016/3/28.
  * 基于情感分析模型的预测方法类
  */
object PredictWithNb{

  /**
    * 模型初始化
    * @param modelPath 模型路径，此路径下包含四个模型
    * @return 模型Map[模型名称，模型]
    * @author zhangxin
    * @note 10
    */
  def init(modelPath: String): Map[String, Any] = {

    //获取模型路径列表
    val fileList = new File(modelPath)
    val modelList = fileList.listFiles()

    //读取模型
    val modelMap = modelList.map(model => {

      //模型名称
      val modelName = model.getName

      //模型
      val tempModelInput = new ObjectInputStream(new FileInputStream(model))

      (modelName, tempModelInput.readObject())
    }).toMap[String, Any]

    modelMap
  }

  /**
    *情感预测   +坤雁分词器
    * @param content 待预测文本
    * @param models  模型Map，由init初始化得到
    * @param stopWords 停用词
    * @return 返回情感label编号
    * @author zhangxin
    * @note 9
    */
  private def predictWithKun(content: Array[String],
                             models: Map[String, Any],
                             stopWords: Array[String]): Double = {

    //对文本[分词+去停]处理
//    val wordSegNoStop = TextPreprocessing.process(content, stopWords, kunConf)

    //用模型对处理后的文本进行预测
    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(content))))

    prediction
  }

  /**
    * 情感预测  +ansj分词器
    * @param content  待预测文章
    * @param models  模型Map[模型名称，模型]，由init初始化得到
    * @param stopWords 停用词
    * @return  返回情感label编号
    * @author zhangxin
    * @note 9
    */
  private def predictWithAnsj(content: String, models: Map[String, Any],
                      stopWords: Array[String]): Double = {

    //对文本[分词+去停]处理
    val wordSegNoStop = TextPreprocessing.process(content, stopWords)

    //用模型对处理后的文本进行预测
    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))

    prediction
  }

  /**
    * 情感预测   +坤雁分词器
    * @param content 文本内容
    * @param model 模型
    * @param stopWords 停用词表
    * @return 预测结果label: neg/neu/pos/neu_pos
    * @author zhangxin
    * @note 10
    */
  def predict(content: Array[String], model: Map[String, Any],
                       stopWords: Array[String]): String = {

    //获取预测结果编号
    val temp = predictWithKun(content, model, stopWords)

    //将编号转成label，作为结果
    val result = temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }

  /**
    * 情感预测   +ansj分词
    * @param content 文本内容
    * @param model 模型
    * @param stopWords 停用词表
    * @return 预测结果label: neg/neu/pos/neu_pos
    * @author zhangxin
    * @note 10
    */
  def predict(content: String, model: Map[String, Any],
                       stopWords: Array[String]): String = {

    //获取预测结果编号
    val temp = predictWithAnsj(content, model, stopWords)

    //将编号转成label，作为结果
    val result = temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }

  /**
    * 二级模型预测  +坤雁分词器
    * @param content  文章内容
    * @param arr  二级模型数组
    * @param stopWords  停用词表
    * @return 情感label
    * @author zhangxin
    * @note 13
    */
  def predictFS(content: Array[String], arr: Array[Map[String, Any]],
                    stopWords: Array[String], kunConf: KunyanConf): String = {

    //先用第一层模型进行第一次预测：neg 或者 其他
    var temp = predictWithKun(content, arr(0), stopWords)

    //若判断为其他，则用第二层进行二次预测：neu 或者 pos
    if (temp == 4.0) {
      temp = predictWithKun(content, arr(1), stopWords)
    }

    val result = temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }

  /**
    *情感预测  返回预测概率
    * @param content 文本内容
    * @param model 模型
    * @param stopWords 停用词表
    * @return Array[(label，预测概率值)]
    * @author zhangxin
    * @note 20
    */
  def predictProbabilities(content: Array[String],
                           model: Map[String, Any],
                           stopWords: Array[String]): Array[(String, String)]= {

    //预处理：分词+去停+格式化
//    val wordSegNoStop = TextPreprocessing.process(content, stopWords)

    //用模型对处理后的文本进行预测，得到预测概率
    val prediction = model("nbModel").asInstanceOf[NaiveBayesModel]
      .predictProbabilities(model("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(model("idfModel").asInstanceOf[IDFModel]
          .transform(model("tfModel").asInstanceOf[HashingTF]
            .transform(content)))).toArray

    //取数据的小数点后四位
    val temp = prediction.map(e => {
      val temp = new BigDecimal(e)
      temp.setScale(4, RoundingMode.FLOOR).toString.toDouble
    })

    //取消科学计数法
    val df = new DecimalFormat("0.0000")

    val result = if(temp(0) >= temp(1)) {
      Array(("neg", df.format(temp(0))), ("neu_pos",  df.format(1 - temp(0))))
    } else{
      Array(("neg", df.format(1 - temp(1))), ("neu_pos",  df.format(temp(1))))
    }

    result
  }
}
