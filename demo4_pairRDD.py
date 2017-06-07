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


    lines = sc.parallelize(["liu simeng","hello world","liu enze"])
    #转化成pairRDD
    pairs = lines.map(lambda x:(x.split(" ")[0],x))
    for line in pairs.collect():
        print line
    #取key
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
    print result.collect()
    for line in result.collect():
        print line
