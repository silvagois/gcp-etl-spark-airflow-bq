#from pyspark import SparkContext, SparkConf
#from pyspark.sql import SQLContext, SparkSession, Row
#from pyspark.sql.types import *
#from pyspark.sql.functions import *
#from pyspark.sql.window import Window
#import sys
import os
import pandas as pd

#from pyspark.sql import SparkSession
#spark = SparkSession.builder\
#        .master('local[*]')\
#        .appName('redemar')\
#        .getOrCreate()\

path = r"C:\Users\marcosgois\Documents\spark\raw"
os.chdir(path)

def read_csv(file_path):
  with open(file_path, 'r') as f:
    print(f.read())
  


pd.read_csv(r"C:\Users\marcosgois\Documents\spark\raw\estoque-2020-01-01.csv",sep=";")
