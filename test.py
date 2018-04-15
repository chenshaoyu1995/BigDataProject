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
# {(0,) : Set(0, 2, 4)}
# Dictionary: Tuple -> Set
funcdepen = {}


# The number of total columns
totalCol = -1

# The dictionary of maximum count
# {Tuple(0, 1) : 1}
# If Tuple is not in the maxcnt, the default value is -1
maxcnt = {}

# The dictionary of distinct count
# Also used to mark that Tuple is non-unique via fd pruning
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
        :param uniqueset: Tuple
        :return:
        '''
        self.uniqueList.append(uniqueset)

    def addnonunique(self, nonuniqueset):
        '''
        The non-unique list are used to generate the possible candidates
        :param nonuniqueset: uple
        :return:
        '''
        self.nonuniqueList.append(nonuniqueset)

    def addminiunique(self, uniqueset):
        """
        After table look-up, the unique combination must be mini-unique
        :param uniqueset: Tuple
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
        '''
        Generator
        :return: Dictionary of Tuple
        '''
        result = {}

        # columnCombination = list(itertools.combinations(list(range(0, totalCol)), self.preLayer.k+1))

        nonuniqueGroup = {}

        for item in self.preLayer.nonuniqueList:
            removeLastItem = item[:-1]
            if removeLastItem in nonuniqueGroup:
                nonuniqueGroup[removeLastItem].append(item[-1])
            else:
                nonuniqueGroup[removeLastItem] = [item[-1]]

        # combination of non unique items from previous layer
        candidateList = []

        for key, value in nonuniqueGroup.items():
            length = len(value)
            for i in range(length - 1):
                for j in range(i + 1, length):
                    if value[i] < value[j]:
                        candidateList.append(key + tuple([value[i]]) + tuple([value[j]]))
                    else:
                        candidateList.append(key + tuple([value[j]]) + tuple([value[i]]))
                        
        for item in candidateList:
            itemtuple = tuple(item)
            if self.isvalidunique(itemtuple):
                continue
            result[itemtuple] = 1

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


def getfuncdepen(colset):
    '''
    Find out the function dependencies in this column combinations
    :param colset: Tuple
    :return: Null
    '''
    fullset = set(colset)
    for colitem in colset:
        leftset = set([colitem])
        rightset = fullset - leftset

        # If the colitem is a single column,
        # the maximum count and distinct count will never be -1

        leftdistcnt = distcnt.get(tuple(leftset), -1)
        fulldistcnt = distcnt.get(tuple(fullset), -1)

        if leftdistcnt == -1:
            continue

        if leftdistcnt == fulldistcnt:
            preset = funcdepen.get(tuple(leftset), set())
            funcdepen[tuple(leftset)] = preset | rightset

    return


def hcaprune(colset):
    '''
    Use HCA to prune the candidate, if return True, the candidate is non-unique set.
    :param candidate: Tuple
    :return: Boolean
    '''

    fullset = set(colset)
    for colitem in colset:
        leftset = set([colitem])
        rightset = fullset - leftset

        # If the colitem is a single column,
        # the maximum count and distinct count will never be -1
        leftmaxcnt = maxcnt.get(tuple(leftset), -1)
        leftdistcnt = distcnt.get(tuple(leftset), -1)
        rightmaxcnt = maxcnt.get(tuple(rightset), -1)
        rightdistcnt = distcnt.get(tuple(rightset), -1)

        if leftdistcnt == -1 or rightdistcnt == -1:
            continue

        if rightdistcnt < leftmaxcnt or leftdistcnt < rightmaxcnt:
            return True

    return False


def fdprune_non_unique(colset, candidates):
    '''
    Use function dependencies to prune
    :param candidates: Dictionary
    :param colset: Tuple
    :return: Null
    '''

    fullset = set(colset)
    for colitem in colset:
        leftset = set([colitem])
        rightlist = list(fullset - leftset)

        rightset = funcdepen.get(tuple(leftset), set())

        for rightitem in rightset:
            candidate = tuple(sorted(rightlist + [rightitem]))
            if candidate not in candidates:
                continue
            if candidate in maxcnt:
                if maxcnt[candidate] != 0:
                    continue
                else:
                    print("The function dependency pruning is wrong! The non-unque",
                          colset, "->", candidate)
                    exit(1)
            if candidate in distcnt:
                if distcnt[candidate] != -1:
                    print("The function dependency pruning is wrong! The non-unque",
                           colset, "->", candidate)
                    exit(1)
            else:
                distcnt[candidate] = -1


    return


def isunique(colset):
    '''
    Look up the table to check whether the column combinations are unique one
    :param colset: Tuple
    :return: Boolean
    '''

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

    nonunique_1_size = len(layers[0].nonuniqueList)
    # For the rest layers
    for i in range(1, nonunique_1_size):
        generator = CandidateGen(layers[i-1])
        kcandidates = generator.create()
        for candidate in kcandidates.keys():
            # Use the HCA to prune the candidate.
            # Add the non-unique item via fd prune
            if hcaprune(candidate) or (candidate in distcnt and distcnt[candidate] == -1):
                layers[i].addnonunique(candidate)
                continue

            # Look up the table to check whether it is unique
            flag = isunique(candidate)

            # After looking up the table, we get the statistic information,
            # so we can find function dependencies from those information
            if i == 1:
                getfuncdepen(candidate)

            if flag:
                layers[i].addminiunique(candidate)
            else:
                layers[i].addnonunique(candidate)
                # Use function dependencies to prune the non-unique candidates
                fdprune_non_unique(candidate, kcandidates)

    print(miniUnique)

    for i in range(0, nonunique_1_size):
        print(layers[i].nonuniqueList)



