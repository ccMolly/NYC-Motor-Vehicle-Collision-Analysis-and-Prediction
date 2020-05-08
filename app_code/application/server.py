from pyspark import SparkContext, SparkConf
import pandas as pd
from app import create_app

def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("test")
    conf.set("spark.driver.allowMultipleContexts", "true")
    sc = SparkContext(conf=conf,pyFiles=['app.py'])
 
    return sc

if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    # Start flask application
    create_app(sc)
    print("done")
