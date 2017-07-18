#coding:utf8
import sys
from pyspark import SparkConf, SparkContext
'''
参考spark快速大数据分析 第13页
从上层来看，每个Spark 应用都由一个驱动器程序（driver program）来发起集群上的各种
并行操作。驱动器程序包含应用的main 函数，并且定义了集群上的分布式数据集，还对这
些分布式数据集应用了相关操作。在前面的例子里，实际的驱动器程序就是Spark shell 本
身，你只需要输入想要运行的操作就可以了。
一旦有了SparkContext，你就可以用它来创建RDD。在例2-1 和例2-2 中，我们调用了
sc.textFile() 来创建一个代表文件中各行文本的RDD。我们可以在这些行上进行各种操
作，比如count()。
'''

if __name__ == "__main__":
    '''
    创建sparkcontext需要传递两个参数
    1 集群URL：告诉Spark 如何连接到集群上。在这几个例子中我们使用的是local，这个
    特殊值可以让Spark 运行在单机单线程上而无需连接到集群。
    2 应用名：在例子中我们使用的是My App。当连接到一个集群时，这个值可以帮助你在
    集群管理器的用户界面中找到你的应用。
    '''
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf = conf)

    ##创建RDD,通过textFile 读取外部文件
    lines_textfile = sc.textFile("/Users/xieenze/Desktop/spark/sparkRDD/test.txt")# 创建一个名为lines的RDD
    print lines_textfile.count()# 统计RDD中的元素个数
    print lines_textfile.first()# 这个RDD中的第一个元素，也就是test.txt的第一行


    #创建RDD,通过parallelize,把程序中一个已有的集合传给sc
    list = ['xieenze','liusimeng']
    lines_parallelize = sc.parallelize(list)
    print lines_parallelize.count()# 统计RDD中的元素个数
    print lines_parallelize.first()# 这个RDD中的第一个元素


    #把RDD 持久化到内存中
    pythonLines.persist


    #行动操作，遍历RDD对像中的数据
    #遍历最近的2行
    for line in lines_textfile.take(2):
        print line
    #遍历最靠前的2行
    for line in lines_textfile.top(2):
        print line
    #遍历全部 返回的是一个list
    for line in lines_textfile.collect():
        print line
    '''
    把数据返回驱动器程序中最简单、最常见的操作是collect()，它会将整个RDD 的内容返
    回。collect() 通常在单元测试中使用，因为此时RDD 的整个内容不会很大，可以放在内
    存中。使用collect() 使得RDD 的值与预期结果之间的对比变得很容易。由于需要将数据
    复制到驱动器进程中，collect() 要求所有数据都必须能一同放入单台机器的内存中。
    take(n) 返回RDD 中的n 个元素，并且尝试只访问尽量少的分区，因此该操作会得到一个
    不均衡的集合。需要注意的是，这些操作返回元素的顺序与你预期的可能不一样。
    这些操作对于单元测试和快速调试都很有用，但是在处理大规模数据时会遇到瓶颈。
    如果为数据定义了顺序，就可以使用top() 从RDD 中获取前几个元素。top() 会使用数据
    的默认顺序，但我们也可以提供自己的比较函数，来提取前几个元素。
    '''



    '''
    最后，关闭Spark 可以调用SparkContext 的stop() 方法，
    '''
    sc.stop()
