from pyspark import SparkContext
import time
import sys
from itertools import combinations

startTime = time.time()

#caseNo = int(sys.argv[1])
#supportVal = int(sys.argv[2])
#inputFile = sys.argv[3]
#outputFile = sys.argv[4]

caseNo=2
supportVal=9
inputFile = './small2.csv'
outputFile = "./output1B.txt"

sc=SparkContext("local[*]","Sample1")
sc.setLogLevel("ERROR")
textData = sc.textFile(inputFile)
firstRow = textData.first()
filteredTextData = textData.filter(lambda x: x!=firstRow)
basicRDD = filteredTextData.map(lambda x: x.split(","))

candidatesList = []
freuqntItemList= []

def aprioriPass1Helper(basket,basketsupportVal,previousCandidates,setSize):
	newCandidates = []
	generalCount = {}
	if len(previousCandidates) == 0:
		return []
	newProspectiveCandidates = []
	for item in combinations(previousCandidates,2):
		newCandidate = set(item[0]).union(set(item[1]))
		if len(newCandidate) == setSize+1:
			newProspectiveCandidates.append(tuple(sorted(newCandidate)))

	newProspectiveCandidates = set(newProspectiveCandidates)

	for p in newProspectiveCandidates:
		for x in basket:
			if set(p).issubset(x[1]):
				generalCount.setdefault(p,0)
				generalCount[p] += 1
	
	for x in generalCount:
		if generalCount[x] >= basketsupportVal:
			newCandidates.append(x)
	return newCandidates

def aprioriPass1(iterator):
    basket = [x for x in iterator]
    lengthBasket = len(basket)
    basketsupportVal = lengthBasket*perUnitsupportVal
    singleCandidateCount = {}
    singleCandidates = []
    finalCandidates = []
    for x in basket:
        for item in x[1]:
            singleCandidateCount.setdefault(item, 0)
            singleCandidateCount[item] += 1
    for x in singleCandidateCount:
        if singleCandidateCount[x]>=basketsupportVal:
            singleCandidates.append(tuple([x]))
    finalCandidates += singleCandidates
    previousCandidates = singleCandidates
    setSize = 1

    while True:
    	newCandidates=aprioriPass1Helper(basket,basketsupportVal,previousCandidates,setSize)
    	finalCandidates += newCandidates
    	previousCandidates = newCandidates
    	setSize += 1
    	if(len(previousCandidates)==0):
    		break
    return finalCandidates

def aprioriPass2(iterator):
    basket = [x for x in iterator]
    candidates = pass2CandidateList.value
    candidateCount = {}
    for a,b in basket:
        for k,v in candidates:
            for item in v:
                if set(item).issubset(set(b)):
                	candidateCount.setdefault(item,0)
                	candidateCount[item] += 1

    for item in sorted(candidateCount.keys()):
        yield (item, candidateCount[item])


if caseNo == 1:
    basketRDD = basicRDD.map(lambda x: (str(x[0]), {str(x[1])})).reduceByKey(lambda x, y: x.union(y))
    basketCount=basketRDD.count()
    perUnitsupportVal = (1.0 * float(supportVal))/basketCount

    tempCandidatesList = basketRDD.mapPartitions(aprioriPass1)
    candidatesList=tempCandidatesList.distinct().groupBy(len).collect()

    pass2CandidateList = sc.broadcast(candidatesList)
    
    pass2CandidateListRDD = basketRDD.mapPartitions(aprioriPass2).reduceByKey(lambda x, y: x + y)
    freuqntItemList=pass2CandidateListRDD.filter(lambda x: x[1] >= supportVal)
    freuqntItemList=freuqntItemList.map(lambda x: x[0]).groupBy(len).collect()

else:
    basketRDD = basicRDD.map(lambda x: (str(x[1]), {str(x[0])})).reduceByKey(lambda x, y: x.union(y))
    basketCount=basketRDD.count()
    perUnitsupportVal = (1.0 * float(supportVal))/basketCount

    tempCandidatesList = basketRDD.mapPartitions(aprioriPass1)
    candidatesList=tempCandidatesList.distinct().groupBy(len).collect()

    pass2CandidateList = sc.broadcast(candidatesList)
    
    pass2CandidateListRDD = basketRDD.mapPartitions(aprioriPass2).reduceByKey(lambda x, y: x + y)
    freuqntItemList=pass2CandidateListRDD.filter(lambda x: x[1] >= supportVal)
    freuqntItemList=freuqntItemList.map(lambda x: x[0]).groupBy(len).collect()



with open(outputFile, 'w') as file:
	file.write('Candidates: \n')
	for a, b in sorted(candidatesList):
		candidates = [x for x in b]
		candidates = sorted(candidates)
		line = ",".join([str(tuple(items)) for items in candidates]).replace(r',)', ')')
		file.write(line + '\n\n')

	file.write('Frequent Itemsets: \n')
	for a, b in sorted(freuqntItemList):
		frequentItemSets = [x for x in b]
		frequentItemSets = sorted(frequentItemSets)
		line = ",".join([str(tuple(items)) for items in frequentItemSets]).replace(r',)', ')')
		file.write(line + '\n\n')

endTime = time.time()
print("Duration: ", endTime-startTime)