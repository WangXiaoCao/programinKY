# 计算曝光度与访问量

标签： smartdata

---
## 1.计算事件的曝光度与访问量

###程序需求

计算每个事件的曝光度与访问量。

事件的曝光度计算自**事件对应的新闻的数目**
事件的访问量计算自**对应新闻被访问的次数**

计算结果保存的时间粒度是每小时。

计算历史数据时，读取的电信数据有些是按照天保存，有些是按照小时保存，预处理方式不同，故独立两个程序：ComputeByHour.scala，ComputeByDay.scala

计算实时数据，也是独立程序：ComputeOnTime


###程序步骤

### EventExposureAndVisit.scala

**步骤一：从mysql读取数据**

1.readEventData方法--读取事件表的数据：事件名称，相关新闻id

2.readNewsData方法--读取新闻表的数据:新闻id, 平台号，url, 时间戳

**步骤二：配对出每个事件所对应的新闻的url和平台**

processData方法

输入：上面读取的两个数据RDD
返回：RDD（事件名称，Array（平台号，url））

**步骤三：预处理电信访问量数据**

访问量的数据已经被计算并保存在hdfs上，程序中只需读取相应事件段的数据，并且做一下预处理，将其转换成（url, 访问次数）的map形式。

processHourlyVisitData方法

输入：直接读取的电信数据
返回：Map[url, 访问次数]

**步骤四：计算事件的曝光度**

 computeExposureDegree方法
 
 输入：事件数据，新闻数据，读取的结束时间（即从什么时候往回读），读取过去数据的小时数。
 返回：（新闻id,(url总数，平台号)）
 
 即，在这个方法中调用了步骤二中的processData方法，并且针对每个新闻，对它的每个平台计算url出现的总数。（在这里简单的对所有平台的url数目进行了求和，也可以赋予平台不同的权重，计算加权总和）
 
 **步骤五：计算事件的访问量**
 
 computeVisitDegree方法
 
 输入：事件数据，新闻数据，电信数据
 返回：（event, visitDegree)
 
 对每个事件中的url， 在预处理后的电信数据中匹配出访问量，然后加总作为该事件的访问量。
 
 **步骤六：保存结果到mysql**
 
  saveResult

##2.计算股票的曝光度与访问量
StockExposureAndVisit.scala

**步骤一：获取数据**

readNewsData方法--读取新闻数据：（url, 时间戳，关联的股票号）

getStockDict方法--获取股票词典： (股票代码， 1）

**步骤二：处理新闻数据**

processNewsData

处理新闻数据,将（url,time,stock1stock2..) => (stock,(url,time),(url,time))的形式
并且补全整个股票词表，没有出现过的股票其value为null

**步骤三：计算曝光度**

computeExposure

统计步骤二的输出，每出现一次url计为一次曝光，统计每只股票的曝光度，并返回（股票号，曝光度）。

**步骤四：预处理电信数据**

processVisitData

将电信数据转变成Map[url, 访问次数]

**步骤五：计算访问量**

computeVisit

统计步骤二的输出，将url与预处理后的电信数据匹配出对应的访问量，将每只股票中的url的访问量求和，作为这只股票的总访问量。

**步骤六：计算访问量趋势**

getLastPeriodsData 从redis获取上一个区间的访问量

computeTrend 计算访问量趋势，比前一天多则为1，少则为-1，相同则为0

**步骤七：整合以上方法**

integration

**步骤八：保存到redis,曝光度，访问量，访问趋势分别存三张表**

sendFinalResults


ComputeByDay：读取按日存储的历史电信访问数据，计算结果按日存储在redis。

ComputeByHour: 读取按小时存储的历史电信访问数据，计算结果按日存储在redis。

ComputeByWeek: 读取redis中按日存储的结果数据，计算结果按周存储在redis。

ComputeByMonth: 读取redis中按日存储的结果数据，计算结果按月存储在redis。

ComputeDayOnTime：实时计算每天的数据，按日存储在redis。

ComputeDayOnTime：实时计算每周的数据，按周存储在redis。

