import numpy as np
import csv
import itertools
from pyspark import SparkContext, SparkConf, StorageLevel

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
lines.persist(StorageLevel.MEMORY_AND_DISK)

# The collection of all the miniUnique
# List of tuple
miniUnique = []

# The collection of all function dependencies
# {(0,) : [(1, 2, 3), (4,)]}
# Dictionary: Tuple -> List of Tuple
funcdepen = {}


# The number of total columns
totalCol = -1

# The dictionary of maximum count
# {Tuple(0, 1) : 1}
# If Tuple is not in the maxcnt, the default value is -1
maxcnt = {}

# The dictionary of maximum count
# {Tuple(0, 1) : 1000}
# If Tuple is not in the distcnt, the default value is -1
distcnt = {}


class ColLayer:
    '''
    Maintain the information for every layer of HAC
    '''

    def __init__(self, k):
        '''
        Initiate the class
        :param k: the layer of the class, which also serves as the length of column combination

        UniqueList: List of tuple. Store the unique combination of this layer.
        NonuniqueList: List of tuple. Store the non-unique combination of this layer.
        MiniUniqueList: List of tuple.
        '''
        self.k = k
        self.uniqueList = []
        self.nonuniqueList = []
        self.miniUniqueList = []

    def addunique(self, uniqueset):
        '''
        The unique list are used to generate the super set of the combination which is definitely unique set
        :param uniqueset: tuple
        :return:
        '''
        self.uniqueList.append(uniqueset)

    def addnonunique(self, nonuniqueset):
        '''
        The non-unique list are used to generate the possible candidates
        :param nonuniqueset: tuple
        :return:
        '''
        self.nonuniqueList.append(nonuniqueset)

    def addminiunique(self, uniqueset):
        """
        After table look-up, the unqiue combination must be mini-unique
        :param uniqueset: tuple
        :return:
        """
        self.miniUniqueList.append(uniqueset)
        miniUnique.append(uniqueset)
        self.addunique(uniqueset)


class CandidateGen:

    def __init__(self, preLayer, Layer):
        '''

        :param preLayer: ColLayer
        '''
        self.preLayer = preLayer
        self.Layer = Layer

    def create(self):
        '''

        :return: List of Tuple
        '''
        result = []

        columnCombination = list(itertools.combinations(list(range(0, totalCol)), self.preLayer.k+1))

        for item in columnCombination:
            itemtuple = tuple(item)
            if self.isvalidunique(itemtuple):
                continue
            if self.hcaprune(itemtuple):
                self.Layer.addnonunique(itemtuple)
                continue
            result.append(itemtuple)
        return result

    # Pruning function
    def isvalidunique(self, candidate):
        '''
        If the candidate is the superset of a minimum unique set, return True
        :param candidate: Tuple
        :return: Boolean
        '''
        fullset = set(candidate)
        for mini in miniUnique:
            if fullset.issuperset(set(mini)):
                return True
        return False

    def hcaprune(self, candidate):
        '''
        Use HCA to prune the candidate, if return True, the candidate is non-unique set.
        :param candidate: Tuple
        :return: Boolean
        '''
        fullset = set(candidate)
        for colitem in candidate:
            leftset = set([colitem])
            rightset = fullset - leftset
            leftmaxcnt = maxcnt.get(tuple(leftset), -1)
            leftdistcnt = distcnt.get(tuple(leftset), -1)
            rightmaxcnt = maxcnt.get(tuple(rightset), -1)
            rightdistcnt = distcnt.get(tuple(rightset), -1)

            if(rightdistcnt == -1 or leftdistcnt == -1):
                continue

            if(rightdistcnt < leftmaxcnt or leftdistcnt < rightmaxcnt):
                return True

        return False

    def getLayer(self):
        return self.Layer

def getfuncdepen():
    return


def isunique(colset):
    linepair = lines.map(lambda line: (tuple((line[i] for i in colset)), 1)) \
                   .reduceByKey(lambda x, y: x + y)

    distcnt[colset] = linepair.count()

    maxitem = linepair.max()

    maxcnt[colset] = maxitem[1]

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

    # For the rest layers
    for i in range(1, totalCol):
        generator = CandidateGen(layers[i-1], layers[i])
        kcandidates = generator.create()
        layers[i] = generator.getLayer()
        for candidate in kcandidates:
            if isunique(candidate):
                layers[i].addminiunique(candidate)
            else:
                layers[i].addnonunique(candidate)

    print(miniUnique)

    # for i in range(0, totalCol):
    #     print(layers[i].nonuniqueList)



