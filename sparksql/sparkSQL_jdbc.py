#coding:utf8
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

"""
sparkSQL读取mysql数据
启动:
  ./bin/spark-submit examples/src/main/python/sql/xxx.py
"""


def jdbc_dataset_example(spark):
    #读取mysql的表
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://127.0.0.1:3306/hib_demo") \
        .option("dbtable", "employee") \
        .option("user", "root") \
        .option("password", "xez199475") \
        .load()

    jdbcDF.show()

    # 创建表
    jdbcDF.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://127.0.0.1:3306/hib_demo") \
        .option("dbtable", "spark_table") \
        .option("user", "root") \
        .option("password", "xez199475") \
        .save()



if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # $example off:init_session$

    jdbc_dataset_example(spark)

    spark.stop()
