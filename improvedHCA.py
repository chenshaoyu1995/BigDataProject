import csv
import time
from math import ceil
import itertools
from pyspark import SparkContext, SparkConf, StorageLevel
from collections import defaultdict
import sys
'''
Spark task initialization.
'''
conf = SparkConf().setMaster("local").setAppName("K-candidate")
sc = SparkContext(conf=conf)

'''
Data initialization.
'''
#lines = sc.textFile("file:///home/sc6439/project/ha.csv")lines = sc.textFile("/user/ecc290/HW1data/open-violations.csv")
#lines = sc.textFile("/user/ecc290/HW1data/open-violations.csv")
lines = sc.textFile(sys.argv[1])
lines = lines.mapPartitions(lambda line: csv.reader(line))
lines.persist(StorageLevel.MEMORY_AND_DISK)

# The collection of all the minimalUniques
# List of tuple, each tuple is the columns of a min-unique
minimalUniques = []

# The collection of all functional dependencies X->Y
# {(0,) : Set(2, 4)}
# Dictionary: Tuple -> Set
functionalDependencies = defaultdict(set)

# The collection of all functional dependencies determinants Y where X->Y
# {(2,) : Set(0), (4,) : Set(0)}
# Dictionary: Tuple -> Set
functionalDependencyDeterminants = defaultdict(set)

# The dictionary of maximum counts
# {Tuple(0, 1) : 1}
# If Tuple is not in the maxCounts, the default value is -1
maxCountsLower = defaultdict(lambda: -1)


# The dictionary of distinct counts
# Also used to mark that Tuple is non-unique via fd pruning
# {Tuple(0, 1) : 1000}
# If Tuple is not in the distinctCounts, the default value is -1
distinctCounts      = defaultdict(lambda: -1)
distinctCountsUpper = defaultdict(lambda: -1)


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

        nonuniqueGroup = defaultdict(list)
        for nonunique in self.preLayer.nonuniqueList:
            nonuniqueGroup[nonunique[:-1]].append(nonunique[-1])

        # combination of non-unique items from previous layer
        candidateList = []

        for key, value in nonuniqueGroup.items():
            length = len(value)
            for i in range(length - 1):
                for j in range(i + 1, length):
                    if value[i] < value[j]:
                        candidateList.append(key + (value[i],) + (value[j],))
                    else:
                        candidateList.append(key + (value[j],) + (value[i],))

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


def getFunctionalDependencies(colSetTuple):
    '''
    Find out the function dependencies in this column combinations
    :param colSetTuple: Tuple represents a column combination
    :return: Null
    '''
    fullDistinctCount = distinctCounts[colSetTuple]
    fullSet = set(colSetTuple)

    for column in colSetTuple:
        leftSet = {column}
        rightSet = fullSet - leftSet

        # If the colitem is a single column,
        # the maximum count and distinct count will never be -1
        leftDistinctCount = distinctCounts[tuple(leftSet)]

        if leftDistinctCount == -1:
            continue

        if leftDistinctCount == fullDistinctCount:
            functionalDependencies[tuple(leftSet)].update(rightSet)
            functionalDependencyDeterminants[tuple(rightSet)].update(leftSet)

def getApproximateDistinct(colSetTuple):
    '''
    Find out the upper bound and lower bound of distinct count
    of this column combination.
    :param colSetTuple: Tuple represents a column combination
    :return: upper bound
    '''
    fullcolUpper = distinctCountsUpper[colSetTuple]

    if fullcolUpper != -1:
        return fullcolUpper

    length = len(colSetTuple)
    fullSet = set(colSetTuple)
    fulllist = list(colSetTuple)

    assert length != 1, "All the tuple with length 1 have distinct count"

    fullcolUpper = totalRow

    for k in range(1, min(int(length / 2.0) + 2, length)):
        subsetList = list(itertools.combinations(fulllist, k))

        for subset in subsetList:

            assert k == len(subset), "It should be the same"

            leftSet = set(subset)
            rightSet = fullSet - leftSet

            leftUpper = getApproximateDistinct(tuple(leftSet))
            rightUpper = getApproximateDistinct(tuple(rightSet))

            fullcolUpper = min(fullcolUpper, leftUpper * rightUpper)

    distinctCountsUpper[colSetTuple] = fullcolUpper

    return distinctCountsUpper[colSetTuple]


def getApproximateMaximum(colSetTuple):
    '''
    Find out the lower bound of maximum count of this column
    combination.
    :param colSetTuple: Tuple represents a column combination
    :return: maximum count lower bound
    '''
    fullcolLower = maxCountsLower[colSetTuple]

    if fullcolLower != -1:
        return fullcolLower

    length = len(colSetTuple)
    fullSet = set(colSetTuple)
    fulllist = list(colSetTuple)

    assert length != 1, "All the tuple with length 1 have maximum count"

    for k in range(1, min(int(length / 2.0) + 2, length)):
        subsetList = list(itertools.combinations(fulllist, k))

        for subset in subsetList:

            assert k == len(subset), "It should be the same 1"

            leftSet = set(subset)
            rightSet = fullSet - leftSet

            leftUpper = getApproximateDistinct(tuple(leftSet))
            rightUpper = getApproximateDistinct(tuple(rightSet))

            leftMaxLower = getApproximateMaximum(tuple(leftSet))
            rightMaxLower = getApproximateMaximum(tuple(rightSet))

            fullcolLower = max(fullcolLower,
                               int(max(ceil(leftMaxLower * 1.0 / rightUpper),
                                       ceil(rightMaxLower * 1.0 / leftUpper)
                                       ))
                               )

    maxCountsLower[colSetTuple] = min(fullcolLower, totalRow)

    return maxCountsLower[colSetTuple]

def hcaPrune(candidate):
    '''
    Use HCA to prune the candidate, if return True, the candidate is non-unique set.
    :param candidate: Tuple represents the column combination of a candidate
    :return: Boolean
    '''

    fullSet = set(candidate)
    for column in candidate:
        leftSet = {column}
        rightSet = fullSet - leftSet

        leftTuple = (column,)
        rightTuple = tuple(rightSet)

        # If the colitem is a single column,
        # the maximum count and distinct count will never be -1

        rightDistinctCount = getApproximateDistinct(rightTuple)
        rightMaxCount = getApproximateMaximum(rightTuple)
        leftMaxCount = getApproximateMaximum(leftTuple)
        leftDistinctCount = getApproximateDistinct(leftTuple)

        assert leftDistinctCount != -1, "the distinct count of a single column should not be -1"

#        if leftdistcnt == -1:
#            continue

        if rightDistinctCount < leftMaxCount or leftDistinctCount < rightMaxCount:
            return True

    return False


def fdPruneNonunique(colSetTuple, candidates):
    '''
    Use function dependencies to prune
    :param candidates: Dictionary whose keys are the candidates
    :param colSetTuple: Tuple represents the column combination of a candidate
    :return: Null
    if {A,X} is a non-unique and X->Y, then {A,Y} is also a non-unique.
    '''

    fullSet = set(colSetTuple)
    for column in colSetTuple:
        X = {column}
        A = list(fullSet - X)

        # obtain the rightSet such that X->Y
        Ys = functionalDependencies[tuple(X)]

        for Y in Ys:
            candidate = tuple(sorted(A + [Y])) #candidate is {A,Y}
            if candidate not in candidates:
                continue
            if candidate in maxCountsLower:
                assert maxCountsLower[candidate] != 1, "the maximum count of a non-unique should not be 1"
                continue
            else:
                if candidate in distinctCounts:
                    assert distinctCounts[candidate] == -1, "the distinct count of a non-unique should be -1"
                else:
                    distinctCounts[candidate] = -1

def fdPruneUnique(colSetTuple, candidates):
    '''
    Use function dependencies to prune
    :param candidates: Dictionary whose keys are the candidates
    :param colSetTuple: Tuple represents the column combination of a candidate
    :return: Null
    if {A,Y} is a unique and X->Y, then {A,X} is also a unique.
    '''
    fullSet = set(colSetTuple)
    for column in colSetTuple:
        Y = {column}
        A = list(fullSet - Y)

        # obtain the determinnants of A
        Xs = functionalDependencyDeterminants[tuple(Y)]

        for X in Xs:
            candidate = tuple(sorted(A+[X])) #candidate is {A,X}
            if candidate not in candidates:
                continue
            else:
                distinctCounts[candidate] = totalRow #candidate is unique

def uniquenessCheck(colSetTuple):
    '''
    Look up the table to check whether the column combinations are unique one
    :param colSetTuple: Tuple represents the column combination of a candidate
    :return: Boolean. True if input is an unique
    '''
    linepair = lines.map(lambda line: (tuple((line[i] for i in colSetTuple)), 1)) \
                    .reduceByKey(lambda x, y: x + y)

    distinctCounts[colSetTuple] = distinctCountsUpper[colSetTuple] = linepair.count() # number of distinct values
    maxitem = linepair.max(key=lambda x:x[1])
    maxCountsLower[colSetTuple] = maxitem[1] # maximum value frequencies

    if maxCountsLower[colSetTuple] == 1:
        assert distinctCounts[colSetTuple]==totalRow, "When maximum count == 1, number of distinct values should be number of rows"
        return True
    else:
        return False


if __name__ == '__main__':
    start = time.time()
    linelist = lines.collect()
    totalRow = len(linelist)
    totalCol = len(linelist[0])

    layers = []

    for i in range(totalCol):
        layers.append(ColLayer(i+1))

    # For the first layer
    for i in range(0, totalCol):
        attriset = tuple([i])
        if uniquenessCheck(attriset):
            layers[0].addMinimalUnique(attriset)
        else:
            layers[0].addNonunique(attriset)

    nonunique_1_size = len(layers[0].nonuniqueList)
    # For the rest layers

    hcapruneCountAll = 0
    fdpruneNonUniqueCountAll=0
    fdpruneUniqueCountAll = 0
    checkCountAll=0

    for i in range(1, nonunique_1_size):
        generator = CandidateGenerator(layers[i-1])
        kcandidates = generator.create()
        hcapruneCount=0
        fdpruneNonUniqueCount=0
        fdpruneUniqueCount = 0
        checkCount=0
        for candidate in kcandidates.keys():
            # Use the HCA to prune the candidate.
            # Add the non-unique item via fd prune
            if candidate in distinctCounts and distinctCounts[candidate] == -1:
                layers[i].addNonunique(candidate)
                fdpruneNonUniqueCount += 1
                continue

            if hcaPrune(candidate):
                layers[i].addNonunique(candidate)
                hcapruneCount+=1
                continue

            # Use functional dependency to prune unique item
            if candidate in distinctCounts and distinctCounts[candidate] == totalRow:
                layers[i].addMinimalUnique(candidate)
                fdpruneUniqueCount += 1
                continue

            # Look up the table to check whether it is unique
            flag = uniquenessCheck(candidate)
            checkCount+=1

            # After looking up the table, we get the statistic information,
            # so we can find function dependencies from those information
            if i == 1:
                getFunctionalDependencies(candidate)

            if flag:
                layers[i].addMinimalUnique(candidate)
                # Use function dependencies to prune the unique candidates
                fdPruneUnique(candidate, kcandidates)
            else:
                layers[i].addNonunique(candidate)
                # Use function dependencies to prune the non-unique candidates
                fdPruneNonunique(candidate, kcandidates)

        hcapruneCountAll += hcapruneCount
        fdpruneNonUniqueCountAll += fdpruneNonUniqueCount
        fdpruneUniqueCountAll += fdpruneUniqueCount
        checkCountAll += checkCount
        # print("{} layer: hca-{},fdnon-{},fdmu-{},check-{}".format(i,hcapruneCount,fdpruneNonUniqueCount, fdpruneUniqueCount,checkCount))
        # sys.stdout.flush()

    end = time.time()
    print('resultStartLine')
    print("time elapsed: {}".format(end - start))
    print("minimalUniques: {}".format(minimalUniques))
    print("hca-{},fdnon-{},fdmu-{},check-{}".format(hcapruneCountAll,fdpruneNonUniqueCountAll, fdpruneUniqueCountAll,checkCountAll))
    print('resultEndLine')
    # for i in range(0, nonunique_1_size):
    #    print(layers[i].nonuniqueList)