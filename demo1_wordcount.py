#coding:utf8
from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
'''
两种写法，一种是通过sparksession，还一种通过sparkcontext
区别：http://blog.csdn.net/u012102306/article/details/53121419
2.0版本之后推荐使用sparksession
'''
'''
运行代码  bin/spark-submit xxx.py 'yyy.txt'
'''
if __name__ == "__main__":
    #控制台接收参数，默认至少为1
    #如果传一个参数，’xxx.txt’ 此时参数数量就是2
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc=SparkContext(appName="PythonStreamingNetworkWordCount")
    lines = sc.textFile(sys.argv[1])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda a,b:a+b)
    output = counts.collect()
    for (word, count) in output:
        print (word,":",count)

'''
from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()
'''
