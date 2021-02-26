# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 18:11:38 2021

@author: lasse
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from MAG import MicrosoftAcademicGraph


if __name__ == '__main__':

  spark = SparkSession \
      .builder \
      .appName("MAG app") \
      .getOrCreate()

  mag = MicrosoftAcademicGraph(spark=spark, data_folderpath="/home/laal/MAG/DATA_SAMPLES/")

  # df = mag.getDataframe('Papers', '/home/laal/MAG/MAGV2/PAPERS/PapersSample.txt')
  df = mag.getDataframe('Papers')

  # df = spark.read.csv('/home/laal/MAG/MAGV2/PAPERS/PapersSample.txt', sep=r"\t", header=False, inferSchema =True)

  print(df.printSchema())
  print(df.show(10))

  print(df.select("CitationCount").show(20))

  most_cited = spark.sql("SELECT * FROM Papers ORDER BY CitationCount DESC LIMIT 50")

  print(most_cited.show(25))

