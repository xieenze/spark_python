#coding:utf8
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
'''
功能：给文本中的数字排序
运行代码  bin/spark-submit xxx.py 'yyy.txt'
'''

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: sort <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonSort")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    sortedCount = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (int(x), 1)) \
        .sortByKey()

    output = sortedCount.collect()
    for (num, unitcount) in output:
        print(num)

    spark.stop()
