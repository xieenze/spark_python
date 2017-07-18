#coding:utf8
from __future__ import print_function
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession
'''
功能：求pi的值
运行代码  bin/spark-submit xxx.py
'''

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    partitions = 5
    n = 100000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        #x**2=x的平方
        return 1 if x ** 2 + y ** 2 <= 1 else 0
    '''
    parallelize第一个参数一是一个 Seq集合
    第二个参数是分区数
    python中 range（1,n+1)生成一个[1-n]的list
    '''
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)

    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()
