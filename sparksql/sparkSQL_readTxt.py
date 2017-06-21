#coding:utf8
from __future__ import print_function

# $example on:init_session$
from pyspark.sql import SparkSession
# $example off:init_session$

# $example on:schema_inferring$
from pyspark.sql import Row
# $example off:schema_inferring$

# $example on:programmatic_schema$
# Import data types
from pyspark.sql.types import *
# $example off:programmatic_schema$

"""
sparkSQL读取txt文件
启动:
  ./bin/spark-submit examples/src/main/python/sql/xxx.py
"""


def schema_inference_example(spark):
    # $example on:schema_inferring$
    #模式推理的案例
    sc = spark.sparkContext

    #导入text文本并缺将每一行转化成row
    lines = sc.textFile("/Users/xieenze/Desktop/spark/sparksql/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # 引入schema, 并且注册 DataFrame 作为一张表.
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")


    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # 返回的结果是DF
    #p是row对象，(row.name row.age)
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)
    # Name: Justin
    # $example off:schema_inferring$




if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # $example off:init_session$

    schema_inference_example(spark)

    spark.stop()
