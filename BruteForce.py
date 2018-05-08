import csv
from pyspark import SparkContext, SparkConf, StorageLevel
import itertools
from collections import defaultdict
import time
import sys

'''
Spark task initialization.
'''

conf = SparkConf().setMaster("local").setAppName("K-candidate")
sc = SparkContext(conf=conf)

'''
Data initialization.
'''
# lines = sc.textFile("/user/hw1651/1w.csv")
#lines = sc.textFile("file:///home/sc6439/project/ha.csv")
#lines = sc.textFile("/user/ecc290/HW1data/open-violations.csv")
lines = sc.textFile(sys.argv[1])
lines = lines.mapPartitions(lambda line: csv.reader(line))
lines.persist(StorageLevel.MEMORY_AND_DISK)

# The collection of all the minimalUniques
# List of tuple, each tuple is the columns of a min-unique
minimalUniques = []

# The number of total columns
totalCol = -1

# The dictionary of maximum counts
# {Tuple(0, 1) : 1}
# If Tuple is not in the maxCounts, the default value is -1
maxCounts = defaultdict(lambda: -1)


class ColLayer:
    '''
    Maintain the information for the k-th layer of Apriori
    '''

    def __init__(self, k):
        '''
        Initiate the class
        :param k: the layer of the class, which also serves as the length of column combination

        uniqueList: List of tuple. Store the uniques of this layer.
        nonuniqueList: List of tuple. Store the non-uniques of this layer.
        minimalUniqueList: List of tuple. Store the minimal-uniques of this layer.
        '''
        self.k = k
        self.uniqueList = []
        self.nonuniqueList = []
        self.minimalUniqueList = []

    def addUnique(self, uniqueSetTuple):
        '''
        The unique list are used to generate the super sets of the combination which is definitely unique set
        :param uniqueSetTuple: Tuple represents the columns of a unique
        :return:
        '''
        self.uniqueList.append(uniqueSetTuple)

    def addNonunique(self, nonuniqueSetTuple):
        '''
        The non-unique list are used to generate the possible candidates of the upper layer
        :param nonuniqueSetTuple: Tuple represents the columns of a non-unique
        :return:
        '''
        self.nonuniqueList.append(nonuniqueSetTuple)

    def addMinimalUnique(self, minUniqueSetTuple):
        """
        After table look-up, the unique combination must be a minimal-unique
        :param minUniqueSetTuple: Tuple represents the columns of a min-unique
        :return:
        """
        self.minimalUniqueList.append(minUniqueSetTuple)
        minimalUniques.append(minUniqueSetTuple)
        self.addUnique(minUniqueSetTuple)


class CandidateGenerator:
    '''
    Generate the candidates that uniqueness checks are needed for one layer of Apriori
    '''

    def __init__(self, preLayer):
        '''
        :param preLayer: ColLayer
        '''
        self.preLayer = preLayer

    def create(self):
        '''
        Generator
        :return: Dictionary of Tuple
        '''
        # combination of non-unique items from previous layer
        candidateList = list(itertools.combinations(list(range(0, totalCol)), self.preLayer.k+1))

        result = {}
        for candidate in candidateList:
            if self.isValidUnique(candidate):
                continue
            result[candidate] = 1

        return result

    # Pruning function
    def isValidUnique(self, candidate):
        '''
        If the candidate is the superset of a minimum unique, return True
        :param candidate: Tuple
        :return: Boolean
        '''
        fullset = set(candidate)
        for minUnique in minimalUniques:
            if fullset.issuperset(set(minUnique)):
                return True
        return False

def uniquenessCheck(colSetTuple):
    '''
    Look up the table to check whether the column combinations are unique one
    :param colSetTuple: Tuple represents the column combination of a candidate
    :return: Boolean. True if input is an unique
    '''

    linepair = lines.map(lambda line: (tuple((line[i] for i in colSetTuple)), 1)) \
        .reduceByKey(lambda x, y: x + y)

    maxitem = linepair.max(lambda x: x[1])
    maxCounts[colSetTuple] = maxitem[1]  # maximum value frequencies

    return maxCounts[colSetTuple] == 1


if __name__ == '__main__':

    start = time.time()

    linelist = lines.collect()

    totalCol = len(linelist[0])

    layers = []

    for i in range(totalCol):
        layers.append(ColLayer(i + 1))

    # For the first layer
    for i in range(0, totalCol):
        attriset = tuple([i])
        if uniquenessCheck(attriset):
            layers[0].addMinimalUnique(attriset)
        else:
            layers[0].addNonunique(attriset)

    nonunique_1_size = len(layers[0].nonuniqueList)

    # For the rest layers
    for i in range(1, nonunique_1_size):
        generator = CandidateGenerator(layers[i - 1])
        kcandidates = generator.create()
        for candidate in kcandidates.keys():
            # Look up the table to check whether it is unique
            if uniquenessCheck(candidate):
                layers[i].addMinimalUnique(candidate)
            else:
                layers[i].addNonunique(candidate)

    end = time.time()
    print("time elapsed: {}".format(end - start))
    print(minimalUniques)
    for i in range(0, nonunique_1_size):
       print(layers[i].nonuniqueList)


