from pyspark import SparkContext
import time
import sys
from itertools import combinations

startTime = time.time()

#filterThreshold = int(sys.argv[1])
#supportVal = int(sys.argv[2])
#inputFile = sys.argv[3]
#outputFile = sys.argv[4]

filterThreshold=20
supportVal=50
inputFile = './ta_feng_all_months_merged.csv'
outputFile = "./output2.txt"

sc=SparkContext("local[*]","Sample2")
sc.setLogLevel("ERROR")
textData = sc.textFile(inputFile)
firstRow = textData.first()

filteredTextData = textData.filter(lambda x: x!=firstRow)
tempRDD = filteredTextData.map(lambda line: line.split(",")).map(lambda x: [item.strip(r'"') for item in x])
basicRDD=tempRDD.map(lambda row: (row[0]+'-'+ str(int(row[1])), str(int(row[5]))))
with open('DATE_CUSTOMER_ID-PRODUCT_ID.csv', "w+") as file:
    file.write("DATE-CUSTOMER_ID,PRODUCT_ID\n")
    for row in basicRDD.collect():
        file.write(",".join([str(item) for item in row]) + '\n')

candidatesList = []
freuqntItemList= []

def aprioriPass1Helper(basket,basketsupportValVal,previousCandidates,setSize):
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
		if generalCount[x] >= basketsupportValVal:
			newCandidates.append(x)
	return newCandidates

def aprioriPass1(iterator):
    basket = [x for x in iterator]
    lengthBasket = len(basket)
    basketsupportValVal = lengthBasket*perUnitsupportValVal
    singleCandidateCount = {}
    singleCandidates = []
    finalCandidates = []
    for x in basket:
        for item in x[1]:
            singleCandidateCount.setdefault(item, 0)
            singleCandidateCount[item] += 1
    for x in singleCandidateCount:
        if singleCandidateCount[x]>=basketsupportValVal:
            singleCandidates.append(tuple([x]))
    finalCandidates += singleCandidates
    previousCandidates = singleCandidates
    setSize = 1

    while True:
    	newCandidates=aprioriPass1Helper(basket,basketsupportValVal,previousCandidates,setSize)
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


basketRDD = basicRDD.map(lambda x: (x[0],{x[1]})).reduceByKey(lambda x, y: x.union(y)).filter(lambda x: len(x[1]) > filterThreshold)
basketCount=basketRDD.count()
perUnitsupportValVal = (1.0 * float(supportVal))/basketCount

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