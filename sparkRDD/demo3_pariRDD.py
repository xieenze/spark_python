#coding:utf8
import sys
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":

    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf = conf)


    lines = sc.parallelize(["liu simeng","hello world","xie enze"])
    #转化成pairRDD
    pairs = lines.map(lambda x:(x.split(" ")[0],x))
    for line in pairs.collect():
        print line
    #取key,value
    for key in pairs.keys().collect():
        print key
    for value in pairs.values().collect():
        print value
    #按key排序
    for line in pairs.sortByKey().collect():
        print line
    #两个rdd 合并
    for line in pairs.join(pairs).collect():
        print line
    #统计词频
    words = lines.flatMap(lambda x:x.split(" "))
    result = words.map(lambda x :(x,1)).reduceByKey(lambda x,y:x+y)
    '''直接打印出rdd.collect(),或者循环打印'''
    print result.collect()
    for line in result.collect():
        print line
