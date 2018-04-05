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
#lines = sc.textFile("open-violations.csv")
lines = sc.textFile("ha.csv")
lines = lines.mapPartitions(lambda line: csv.reader(line))
lines.cache()

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

    def addunique(self, uniqueset):
        '''
        The unique list are used to generate the super set of the combination which is definitely unique set
        :param uniqueset:
        :return:
        '''
        self.uniqueList.append(uniqueset)

    def addnonunique(self, nonuniqueset):
        '''
        The non-unique list are used to generate the possible candidates
        :param nonuniqueset:
        :return:
        '''
        self.nonuniqueList.append(nonuniqueset)

    def addminiunique(self, uniqueset):
        """
        After table look-up, the unqiue combination must be mini-unique
        :param uniqueset:
        :return:
        """
        self.miniUniqueList.append(uniqueset)
        miniUnique.append(uniqueset)
        self.addunique(uniqueset)


class CandidateGen:

    def __init__(self, preLayer):
        '''

        :param preLayer: ColLayer
        '''
        self.preLayer = preLayer

    def create(self):
        result = []

        columnCombination = list(itertools.combinations(list(range(0, totalCol)), self.preLayer.k+1))

        for item in columnCombination:
            itemset = set(item)
            if self.isvalidunique(itemset):
                continue
            result.append(itemset)
        return result

    # Pruning function
    def isvalidunique(self, candidate):
        for mini in miniUnique:
            if candidate.issuperset(mini):
                return True
        return False


def isunique(colset):
    linepair = lines.map(lambda line: (tuple([line[i] for i in colset]), 1)) \
                   .reduceByKey(lambda x, y: x + y)

    distcnt[colset] = linepair.count()

    maxitem = linepair.max()

    maxcnt[colset] = maxitem[1]

    print(maxcnt[colset])

    print(linepair.top(10))

    return maxcnt[colset] == 1


if __name__ == '__main__':

    linelist = lines.collect()

    totalCol = len(linelist[0])

    layers = []

    for i in range(totalCol):
        layers.append(ColLayer(i+1))

    # For the first layer

    for i in range(0, totalCol):
        attriset = tuple([i])
        if isunique(attriset):
            layers[0].addminiunique(attriset)
        else:
            layers[0].addnonunique(attriset)
    #
    # # For the rest layers
    # for i in range(1, totalCol):
    #     generator = CandidateGen(layers[i-1])
    #     kcandidate = generator.create()
    #     print(len(kcandidate))
    #     if len(kcandidate) != 0:
    #         print(kcandidate[0])



