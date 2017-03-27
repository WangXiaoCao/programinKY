# 计算关联属性与事件

标签： smartdata

---

##程序需求：

分别计算出每个股票，概念，行业，事件对应的最相关的股票，概念，行业，事件。

##程序步骤：

**步骤一：**

从mysql中分别读取两个数据：

方法readNewsData--读取新闻数据：（新闻id, 股票号，概念，行业）
方法readEventData--读取事件数据：（事件id, 新闻id）

**步骤二：propertyAsKey方法**

将新闻数据转变为以property为key， Array[newsId]为value的格式

将事件数据转变为以eventid为key, Array[newsId]为value的格式

将以上两个数据Union。value都是Array[newsId],这样，只需要通过newsId找到对应的属性与事件，就形成了四四对应的关系。

**步骤三：newsAsKey方法**

将新闻数据和事件数据都转变为以newss_id为key，属性或事件id为value的格式。并且转换成map.

**步骤四：getRelation方法**

调用步骤二，三的方法，对propertyAsKey的输出中的value,将id转换成对应的属性与事件id,对value中的数据进行分类计数，分别提取最高频次的属性于事件。

**步骤五：computeAndSaveToMysql**

将结果保存到mysql中的事件表与股票表中。





