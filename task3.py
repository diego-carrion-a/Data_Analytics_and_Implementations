from __future__ import print_function

import sys

from pyspark import SparkContext

import re
import numpy as np


if __name__ == "__main__":
    if len(sys.argv) !=4:
        print("Usage: A1Script <file> <output> ", file=sys.stderr) ### how to use the file on spark-submit
        exit(-1)
        
    sc= SparkContext()     
    wikiCategoryLinks= sc.textFile(sys.argv[1], 1)
    wikiPages= sc.textFile(sys.argv[2], 1)
    
    wikiCats=wikiCategoryLinks.map(lambda x: x.split(",")).map(lambda x: (x[0].replace('"', ''), x[1].replace('"', '') ))
    
    # Assumption: Each document is stored in one line of the text file
    # We need this count later ... 
    numberOfDocs = wikiPages.count()

    # Each entry in validLines will be a line from the text file
    validLines = wikiPages.filter(lambda x : 'id' in x and 'url=' in x)

    # Now, we transform it into a set of (docID, text) pairs
    keyAndText = validLines.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6])) 
    
    # The following function gets a list of dictionaryPos values,
    # and then creates a TF vector
    # corresponding to those values... for example,
    # if we get [3, 4, 1, 1, 2] we would in the
    # end have [0, 2/5, 1/5, 1/5, 1/5] because 0 appears zero times,
    # 1 appears twice, 2 appears once, etc.
    
    def buildArray(listOfIndices):
        
        returnVal = np.zeros(20000)
        
        for index in listOfIndices:
            returnVal[index] = returnVal[index] + 1
        
        mysum = np.sum(returnVal)
        
        returnVal = np.divide(returnVal, mysum)
        
        return returnVal
    
    # Cosine Similarity of two vectors 
    def cousinSim (x, y):
    	normA = np.linalg.norm(x)
    	normB = np.linalg.norm(y)
    	return np.dot(x,y)/(normA*normB)
    
    # Now, we transform it into a set of (docID, text) pairs
    keyAndText = validLines.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6]))
    
    # Now, we split the text in each (docID, text) pair into a list of words
    # After this step, we have a data set with
    # (docID, ["word1", "word2", "word3", ...])
    # We use a regular expression here to make
    # sure that the program does not break down on some of the documents
    
    regex = re.compile('[^a-zA-Z]')
    
    # remove all non letter characters
    keyAndListOfWords = keyAndText.map(lambda x : (str(x[0]), regex.sub(' ', x[1]).lower().split()))
    # better solution here is to use NLTK tokenizer
    
    # Now get the top 20,000 words... first change (docID, ["word1", "word2", "word3", ...])
    # to ("word1", 1) ("word2", 1)...
    allWords = keyAndListOfWords.flatMap(lambda p: ((x,1) for x in p[1]))
    
    # Now, count all of the words, giving us ("word1", 1433), ("word2", 3423423), etc.
    allCounts = allWords.reduceByKey(lambda x,y : x+y)
    
    # Get the top 20,000 words in a local array in a sorted format based on frequency
    topWords = allCounts.top(20000 , lambda p: p[1])
    
    
    # We'll create a RDD that has a set of (word, dictNum) pairs
    # start by creating an RDD that has the number 0 through 20000
    # 20000 is the number of words that will be in our dictionary
    topWordsK = sc.parallelize(range(20000))
    
    # Now, we transform (0), (1), (2), ... to ("MostCommonWord", 1)
    # ("NextMostCommon", 2), ...
    # the number will be the spot in the dictionary used to tell us
    # where the word is located
    dictionary = topWordsK.map (lambda x : (topWords[x][0], x))
    
    ################### TASK 2  ##################

    # Next, we get a RDD that has, for each (docID, ["word1", "word2", "word3", ...]),
    # ("word1", docID), ("word2", docId), ...
    
    allWordsWithDocID = keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1]))
    
    # Now join and link them, to get a set of ("word1", (dictionaryPos, docID)) pairs
    allDictionaryWords = dictionary.join(allWordsWithDocID)
    
    #allDictionaryWords.cache()
    # Now, we drop the actual word itself to get a set of (docID, dictionaryPos) pairs
    justDocAndPos = allDictionaryWords.map(lambda p: (p[1][1],p[1][0]))
    
    # Now get a set of (docID, [dictionaryPos1, dictionaryPos2, dictionaryPos3...]) pairs
    allDictionaryWordsInEachDoc = justDocAndPos.groupByKey()
    
    # The following line this gets us a set of
    # (docID,  [dictionaryPos1, dictionaryPos2, dictionaryPos3...]) pairs
    # and converts the dictionary positions to a bag-of-words numpy array...
    allDocsAsNumpyArrays = allDictionaryWordsInEachDoc.map(lambda x: (x[0], buildArray(x[1])))
     
    # Now, create a version of allDocsAsNumpyArrays where, in the array,
    # every entry is either zero or one.
    # A zero means that the word does not occur,
    # and a one means that it does.
    
    zeroOrOne = allDocsAsNumpyArrays.map(lambda p: (p[0], np.clip(np.multiply(p[1],9e10),0,1)))
    
    # Now, add up all of those arrays into a single array, where the
    # i^th entry tells us how many
    # individual documents the i^th word in the dictionary appeared in
    dfArray = zeroOrOne.reduce(lambda x1, x2: ("", np.add(x1[1], x2[1])))[1]
    
    # Create an array of 20,000 entries, each entry with the value numberOfDocs (number of docs)
    multiplier = np.full(20000, numberOfDocs)
    
    # Get the version of dfArray where the i^th entry is the inverse-document frequency for the
    # i^th word in the corpus
    idfArray = np.log(np.divide(np.full(20000, numberOfDocs), dfArray))
    
    # Finally, convert all of the tf vectors in allDocsAsNumpyArrays to tf * idf vectors
    allDocsAsNumpyArraysTFidf = allDocsAsNumpyArrays.map(lambda x: (x[0], np.multiply(x[1], idfArray)))
    

    ################### TASK 3  ##################
    
    # Finally, we have a function that returns the prediction for the label of a string, using a kNN algorithm
    def getPrediction (textInput, k):
        k= int(k)
        # Create an RDD out of the textIput
        myDoc = sc.parallelize (('', textInput))
    
        # Flat map the text to (word, 1) pair for each word in the doc
        wordsInThatDoc = myDoc.flatMap (lambda x : ((j, 1) for j in regex.sub(' ', x).lower().split()))
    
        # This will give us a set of (word, (dictionaryPos, 1)) pairs
        allDictionaryWordsInThatDoc = dictionary.join (wordsInThatDoc).map (lambda x: (x[1][1], x[1][0])).groupByKey ()
    
        # Get tf array for the input string
        myArray = buildArray (allDictionaryWordsInThatDoc.top (1)[0][1])
    
        # Get the tf * idf array for the input string
        myArray = np.multiply (myArray, idfArray)
        
        
        #### step to avoid the join, instead we take the dot product to the TFI array
        allDocsAsNumpyArraysTFidfDotP=allDocsAsNumpyArraysTFidf.map(lambda p: (p[0], np.dot(p[1],myArray)))
        ### we filter all documents that have those words
        allDocsMatch=allDocsAsNumpyArraysTFidfDotP.filter(lambda p: p[1]>0)
        
        # now we join those documents with the categories waaaay smaller 720 vs 13780
        distances = wikiCats.join(allDocsMatch).map(lambda p: (p[1][0],(p[1][1],1)))
        ## normalized distances
        distances = distances.reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1])).map(lambda p: (p[0],(p[1][0]/p[1][1])))
        
        # distances = allDocsAsNumpyArraysTFidf.map (lambda x : (x[0], cousinSim (x[1],myArray)))
        # get the top k distances
        topK = distances.top (k, lambda x : x[1])
        
        # and transform the top k distances into a set of (docID, 1) pairs
        docIDRepresented = sc.parallelize(topK).map (lambda x : (x[0], 1))
    
        # now, for each docID, get the count of the number of times this document ID appeared in the top k
        # numTimes = docIDRepresented.reduceByKey(lambda a, b: a+b)
        numTimes = docIDRepresented.reduceByKey(lambda a, b: a+b)
        
        # Return the top 1 of them.
        return numTimes.top(k, lambda x: x[1])
    
    out = []

    pred1 = getPrediction('Sport Basketball Volleyball Soccer', 10)
    out.append(pred1)

    pred2 = getPrediction('What is the capital city of Australia?', 10)
    out.append(pred2)

    pred3 = getPrediction('How many goals Vancouver score last year?', 10)
    out.append(pred3)
    
    sc.parallelize(out).repartition(1).saveAsTextFile(sys.argv[3])
    
    
    sc.stop()