#coding:utf8
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
'''
 条件查询简单案例，使用了join和where查询
'''

def basic_df_example(spark):
    #读取json
    df1 = spark.read.json("/Users/xieenze/Desktop/spark/sparksql/people.json")
    df2 = spark.read.load('/Users/xieenze/Desktop/spark/sparksql/people0.json',format='json')
    #创建局部临时视图
    df1.createOrReplaceTempView("p1")
    df2.createOrReplaceTempView('p2')
    #where查询
    sqlDF = spark.sql("SELECT p1.name name,p1.age age FROM p1 ,p2 where p1.age=p2.age")
    sqlDF.show()
    #join查询
    spark.sql('SELECT p1.name name,p1.age age FROM p1 inner join p2 on p1.age=p2.age').show()
    #将查询结果导出成parquet格式
    #df1.write.save('/Users/xieenze/Desktop/spark/sparksql/export.parquet')



    '''
    Parquet是面向分析型业务的列式存储格式，由Twitter和Cloudera合作开发
    在hadoop生态圈的快速发展过程中，涌现了一批开源的数据分析引擎，例如Hive、Spark SQL、Impala、Presto等，
    同时也产生了多个高性能的列式存储格式，例如RCFile、ORC、Parquet等
    列式存储和行式存储相比有哪些优势呢？
    1 可以跳过不符合条件的数据，只读取需要的数据，降低IO数据量。
    2 压缩编码可以降低磁盘存储空间。由于同一列的数据类型是一样的，可以使用更高效的压缩编码（例如Run Length Encoding和Delta Encoding）进一步节约存储空间。
    3 只读取需要的列，支持向量运算，能够获取更好的扫描性能。
    '''


if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    basic_df_example(spark)

    spark.stop()
