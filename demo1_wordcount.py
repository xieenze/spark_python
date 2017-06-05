import sys

from pyspark import SparkContext

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "WordCount")
    lines = sc.parallelize(["pandas", "i like pandas"])
    #countByValue:各元素在RDD 中出现的次
    result = lines.flatMap(lambda x: x.split(" ")).countByValue()
    for key, value in result.iteritems():
        print  (key, value)
