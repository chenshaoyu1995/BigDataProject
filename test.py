import numpy as np
import csv
import itertools
from pyspark import SparkContext, SparkConf

# The collection of all the miniUnique
miniUnique = []

# The number of total columns
totalCol = -1

# The dictionary of maximum count
# {Set(0, 1) : 1}
maxcnt = {}

# The dictionary of maximum count
# {Set(0, 1) : 1000}
distcnt = {}


class ColLayer:
    '''
    Maintain the information for every layer of HAC
    '''

    def __init__(self, k):
        '''
        Initiate the class
        :param k: the layer of the class, which also serves as the length of column combination

        UniqueList: List of set. Store the unique combination of this layer.
        NonuniqueList: List of set. Store the non-unique combination of this layer.
        MiniUniqueList: List of set.
        '''
        self.k = k
        self.uniqueList = []
        self.nonuniqueList = []
        self.miniUniqueList = []

    def addUnique(self, uniqueset):
        '''
        The unique list are used to generate the super set of the combination which is definitely unique set
        :param uniqueset:
        :return:
        '''
        self.uniqueList.append(uniqueset)

    def addNonUnique(self, nonuniqueset):
        '''
        The non-unique list are used to generate the possible candidates
        :param nonuniqueset:
        :return:
        '''
        self.nonuniqueList.append(nonuniqueset)

    def addMiniUnique(self, uniqueset):
        """
        After table look-up, the unqiue combination must be mini-unique
        :param uniqueset:
        :return:
        """
        self.miniUniqueList.append(uniqueset)
        miniUnique.append(uniqueset)
        self.addUnique(uniqueset)


class CandidateGen:

    def __init__(self, preLayer):
        '''

        :param preLayer: ColLayer
        '''
        self.preLayer = preLayer


    def create(self):
        columnCombination = []

        columnCombination = list(itertools.combinations(list(range(0, totalCol)), self.preLayer.k))

        return columnCombination

    def isvalidUnique(self, candidate):
        for mini in miniUnique:
            if candidate.issuperset(mini):
                return True
        return False


if __name__ == '__main__':

    conf = SparkConf().setMaster("local").setAppName("K-candidate")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("open-violations.csv")
    lines = lines.mapPartitions(lambda line: csv.reader(line))
    lines.cache()

    linelist = lines.collect()

    totalCol = len(linelist[0])

    layers = []

    for i in range(totalCol):
        layers.append(ColLayer(i+1))

    for i in range(totalCol):
        generator = CandidateGen(layers[i])
        print(len(generator.create()))



