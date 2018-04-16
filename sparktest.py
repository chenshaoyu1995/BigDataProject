import numpy as np
import csv
import itertools
from pyspark import SparkContext, SparkConf

'''
Spark task initialization.
'''
conf = SparkConf().setMaster("local").setAppName("K-candidate")
sc = SparkContext(conf=conf)

'''
Data initialization.
'''
lines = sc.textFile("ha.csv")
lines = lines.mapPartitions(lambda line: csv.reader(line))
lines.cache()

testset = set(list([0, 1]))

res = lines.map(lambda line: (tuple([line[i] for i in testset]), 1))

print(res.count())

linelist = res.collect()

for i in range(10):
    print(linelist[i])