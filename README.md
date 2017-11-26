# Project1
##需求1：针对股票新闻数据集中的新闻标题,编写WordCount程序,统计所有除Stop-word(如“的”,“得”,“在”等)出现次数k次以上的单词计数,最后的结果按照词频从高到低排序输出。
###设计思路：选择了IK-Analyser中文分词器，IK支持用户自定义停词和拓展词，如下图所设置：
![https://github.com/wangyupush/Project1/blob/master/3-1.png]
启动了自定义词典：
![https://github.com/wangyupush/Project1/blob/master/1.png]

然后我是将每条value里面的值通过scanner工具提取出需要的标题内容。然后对标题进行wordcount：
![https://github.com/wangyupush/Project1/blob/master/3-2.png]
当对标题进行了词频统计后，对它们进行由大到小排序，设置参数k：
![https://github.com/wangyupush/Project1/blob/master/3-4.png]
运行时的参数设置：
![https://github.com/wangyupush/Project1/blob/master/1-3.png]
运行过程：
![https://github.com/wangyupush/Project1/blob/master/1-2.png]
运行结果：
![https://github.com/wangyupush/Project1/blob/master/1-4.png]

##需求2：针对股票新闻数据集,以新闻标题中的词组为key,编写带URL属性的文档倒排索引程序,将结果输出到指定文件。
###设计思路：同第一个需求在wordcount部分一样，通过scanner工具提取出需要的标题内容，求出标题的词组：
![https://github.com/wangyupush/Project1/blob/master/4-1.png]
中间过程和最后的输出都是 filename@url@wordcount的结构

运行时的参数设置：
![https://github.com/wangyupush/Project1/blob/master/2-1.png]
运行过程：
![https://github.com/wangyupush/Project1/blob/master/2-2.png]
运行结果：
![https://github.com/wangyupush/Project1/blob/master/3-3.png]
