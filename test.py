import numpy as np
import csv
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("K-candidate")
sc = SparkContext(conf = conf)

lines = sc.textFile("open-violations.csv")
lines = lines.mapPartitions(lambda line: csv.reader(line))
lines.cache()

linelist = lines.collect()

print(len(linelist))
print(len(linelist[0]))
print(linelist[0])


