# rank--代码解析

标签（空格分隔）： smartdata

---

##PageRnak
pagerank的实现主要由两个方法组成：创建图；隐马尔科夫链迭代。
###buildGraph
buildGraph方法用于创建顶点与边的关系图。

输入的参数有两个：
sc:SparkContext
lines:一组（顶点，相邻点）


输出返回的是：
（顶点，（相邻点1，相邻点2...相邻点n）

这个方法的目的其实就是将每个顶点都作为一个key,并找出被它指向的所有点的list作为它的value。如果有顶点没有任何出度，那么list为空。

###computing
computing方法用于迭代计算评分，这里设置每个顶点的初始值为1.被指向的链接越多（也就是入度越多）那么该顶点的得分也会随之越高。

输入的参数是三个：
sc:SparkContext
linkList 点与点之间的图关系,也就是上一个方法的输出作为这个方法的输入图。
iteration 迭代的次数

输出的数据是：
(顶点，得分）  并且按照得分进行了降序排序。

##TextRank
textrank的思路与pagerank是一致的，只是在创建关系图的时候是从文本中寻找词与词之间的关系。顶点是文章分词后的“词”个体。

###buildRelation
buildRelation方法是创建图。

这里的输入参数是：
text 一篇分词后由“词”组成的array
size 窗口的大小，因为我们假设在这个窗口中的词具备相邻关系，一般窗口大小设为5.

输出是：
（词，（相邻词1，相邻词2,...,相邻词n））

具体做法是，对文章中的每个词进行遍历，并提取出这个词前后（size-1）个词作为它的相邻词。

###computing
computing方法与pagerank是一致的。将输入的关系图进行迭代计算，直至收敛，计算出每个词的得分。

输入参数是：
sc SparkContext
data 每篇文章的图结构
iteration 迭代的次数

输出返回：
（词，评分），并且按照得分降序排列。






