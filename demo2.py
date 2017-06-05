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

    ##创建RDD,通过textFile 读取外部文件
    lines_textfile = sc.textFile("README.md")# 创建一个名为lines的RDD
    print str(lines_textfile.count())+"==================="# 统计RDD中的元素个数
    print str(lines_textfile.first())+"==================="# 这个RDD中的第一个元素，也就是README.md的第一行


    #创建RDD,通过parallelize,把程序中一个已有的集合传给sc
    list = ['xieenze','liusimeng']
    lines_parallelize = sc.parallelize(list)
    print str(lines_parallelize.count())+"==================="# 统计RDD中的元素个数
    print str(lines_parallelize.first())+"==================="# 这个RDD中的第一个元素，也就是README.md的第一行




    #筛选出包含python的行,filter 转化操作
    pythonLines = lines_textfile.filter(lambda line: "Jav" in line)
    print str(pythonLines.first())+"==================="
    #把RDD 持久化到内存中
    pythonLines.persist
    print str(pythonLines.count())+"=============="


    #行动操作，遍历RDD对像中的数据
    #遍历几行
    for line in lines_textfile.take(10):
        print line
    #遍历全部
    for line in lines_textfile.collect():
        print line
