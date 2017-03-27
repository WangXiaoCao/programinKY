# 计算搜索词cosine相似度

标签： smartdata

---

该工程内容为根据电信返回的搜索词，计算词与词之间的两两cosine相似度。
时间粒度为24小时。

**程序的大致步骤如下**

1.首先readData方法从hdfs上读取过去24小时的搜索词数据。

2.提取id,搜索词两个字段之后，使用urlcodeProcess方法对utf-16编码的搜索词进行解码

3.通过reducebykey，将同一个用户的搜索词根据id分组。

4.对搜索词做一系列预处理（去停，删除异常等）

5.调用computeCosineByRDD方法进行搜索词的cosine相似度计算

6.保存结果到hdfs。

**配置的参数如下**

stopPuncPath 停用词文件的路径

val rawDataPath 电信数据文件路径

val outputCosine  保存结果的路径

val para 支持度，分区数，shuffle时的分区数









