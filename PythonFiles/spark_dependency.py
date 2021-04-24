"""
spark.py
~~~~~~~~
Module containing helper function for use with Apache Spark
"""



from os import environ, listdir, path
import json
import findspark

import time

# Initialize Findspark to locate Pyspark installed in the system
findspark.init()

# Import Pyspark libraries and functions
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext , SparkSession




def start_spark(app_name='LF_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """
    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """
   
    sc = SparkContext(conf=SparkConf().setAppName(app_name).setMaster(master))
    sqlc=SQLContext(sc)
    sc.setLogLevel('ERROR')
    

    #spark_logger.warn('no config file found')
    
    return sc, sqlc

def stop_spark(spark_ctx):
    spark_ctx.stop()

sc, sqlc = start_spark()
stop_spark(sc)

