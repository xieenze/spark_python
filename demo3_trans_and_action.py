#coding:utf8
import sys

#from pyspark import SparkContext
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":
    #master = "local"
    #if len(sys.argv) == 2:
    #    master = sys.argv[1]
    #sc = SparkContext(master, "aaaaaaaaaaaaa")

    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf = conf)

    #转化操作map() 接收一个函数，把这个函数用于RDD中的每个元素，将函数的返回结果
    #作为结果RDD 中对应元素的值。
    #而转化操作filter()则接收一个函数，并将RDD中满足该函数的元素放入新的RDD 中返回。
    list = [1,2,3,4]
    nums = sc.parallelize(list)
    squared = nums.map(lambda x:x*x).collect()
    for line in squared:
        print line

    #有时候，我们希望对每个输入元素生成多个输出元素。实现该功能的操作叫作flatMap()。
    #和map() 类似，我们提供给flatMap() 的函数被分别应用到了输入RDD 的每个元素上。不
    #过返回的不是一个元素，而是一个返回值序列的迭代器。输出的RDD 倒不是由迭代器组
    #成的。我们得到的是一个包含各个迭代器可访问的所有元素的RDD。flatMap() 的一个简
    #单用途是把输入的字符串切分为单词
    lines = sc.parallelize(["hello world","liusimeng"])
    words = lines.flatMap(lambda line:line.split(" "))
    print words.first()  #hello
    print words.count()  #3


    #数据去重
    distnct_list = [1,2,3,4,1]
    d_rdd = sc.parallelize(distnct_list).distinct()
    print str(d_rdd.count())+ "-----------------"



    '''把数据返回驱动器程序中最简单、最常见的操作是collect()，它会将整个RDD 的内容返
    回。collect() 通常在单元测试中使用，因为此时RDD 的整个内容不会很大，可以放在内
    存中。使用collect() 使得RDD 的值与预期结果之间的对比变得很容易。由于需要将数据
    复制到驱动器进程中，collect() 要求所有数据都必须能一同放入单台机器的内存中。
    take(n) 返回RDD 中的n 个元素，并且尝试只访问尽量少的分区，因此该操作会得到一个
    不均衡的集合。需要注意的是，这些操作返回元素的顺序与你预期的可能不一样。
    这些操作对于单元测试和快速调试都很有用，但是在处理大规模数据时会遇到瓶颈。
    如果为数据定义了顺序，就可以使用top() 从RDD 中获取前几个元素。top() 会使用数据
    的默认顺序，但我们也可以提供自己的比较函数，来提取前几个元素。'''
