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
sparkSQL读取json文件
启动:
  ./bin/spark-submit examples/src/main/python/sql/xxx.py
"""


def basic_df_example(spark):
    #读取json
    #sparkSQL RDD
    #两种读取json的方式
    df = spark.read.json("/Users/xieenze/Desktop/spark/sparksql/people.json")
    df = spark.read.load("/Users/xieenze/Desktop/spark/sparksql/people.json",format='json')

    # 显示DataFrame的内容到stdout
    df.show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+
    # $创建df的例子


    #按照树的模式打印字段
    #类似于describe table
    df.printSchema()
    # root
    # |-- age: long (nullable = true)
    # |-- name: string (nullable = true)

    # 只选择name这一列
    df.select("name").show()
    # +-------+
    # |   name|
    # +-------+
    # |Michael|
    # |   Andy|
    # | Justin|
    # +-------+

    #选择每一个人，但是年龄+1
    df.select(df['name'], df['age'] + 1).show()
    # +-------+---------+
    # |   name|(age + 1)|
    # +-------+---------+
    # |Michael|     null|
    # |   Andy|       31|
    # | Justin|       20|
    # +-------+---------+

    # 筛选年龄大于21的，用filter过滤器
    df.filter(df['age'] > 21).show()
    # +---+----+
    # |age|name|
    # +---+----+
    # | 30|Andy|
    # +---+----+

    # 按照年龄统计每个人
    df.groupBy("age").count().show()
    # +----+-----+
    # | age|count|
    # +----+-----+
    # |  19|    1|
    # |null|    1|
    # |  30|    1|
    # +----+-----+
    # 上面的都是调用api

    # 现在直接写sql查询
    # Register the DataFrame as a SQL temporary view
    #创建局部临时视图
    df.createOrReplaceTempView("people")

    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+
    # $example off:run_sql$

    # $example on:global_temp_view$
    # Register the DataFrame as a global temporary view
    #创建全局临时视图  只支持当前的session查询
    df.createGlobalTempView("people")

    # Global temporary view is tied to a system preserved database `global_temp`
    #全局临时视图与系统保留的数据库“global_temp”有关 支持多个session查询
    spark.sql("SELECT * FROM global_temp.people").show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+

    # Global temporary view 支持多个session查询
    #创建新session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+
    # $example off:global_temp_view$




if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # $example off:init_session$

    basic_df_example(spark)

    spark.stop()
