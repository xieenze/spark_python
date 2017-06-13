#coding:utf8
#http://www.cnblogs.com/vovlie/p/4178077.html
from __future__ import print_function
from flask import Flask, jsonify
from flask import abort
from flask import make_response

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]

@app.route('/todo/api/v1.0/tasks', methods=['GET'])
def get_tasks():
    spark = getspark()
    a,b=basic_df_example(spark)
    return jsonify({'tasts':b})

@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task = filter(lambda t: t['id'] == task_id, tasks)
    if len(task) == 0:
        abort(404)
    return jsonify({'task': task[0]})

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


def basic_df_example(spark):

    # $example on:create_df$
    # spark is an existing SparkSession
    df = spark.read.json("/home/xieenze/Desktop/spark/testweet.json")
    # Displays the content of the DataFrame to stdout
    a=df.collect()
    df.createGlobalTempView("people")

    # Global temporary view is tied to a system preserved database `global_temp`
    b=spark.sql("SELECT * FROM global_temp.people").collect()

    return a,b;

def getspark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark



if __name__ == '__main__':

    app.run(debug=True)
