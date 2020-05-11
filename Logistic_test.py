import sys
import re
import numpy as np
from operator import add
from pyspark import SparkContext
from pyspark import SparkConf

def freqArray (listOfIndices, numberofwords):
    returnVal = np.zeros (20000)
    for index in listOfIndices:
        returnVal[index] = returnVal[index] + 1
    returnVal = np.divide(returnVal, numberofwords)
    return returnVal

def f1_measure(tupl):
    result=[]
    precision = tupl[0]/(tupl[0]+tupl[2])
    recall = tupl[0]/(tupl[0]+tupl[3])
    if (precision+recall) == 0:
        result.append(0)
    else:
        f = (2 * precision * recall) / (precision+recall)
        result.append(f)
    return result

if __name__ == "__main__":
    if len(sys.argv) !=5:
        print("Usage: A1Script <file> <output> ", file=sys.stderr) ### how to use the file on spark-submit
        exit(-1)

    sc= SparkContext() 
    coefficients = sc.textFile(sys.argv[1], 1) ## read the coefficients input file
    
    coefficients= coefficients.map(lambda x: float(x)) ## get the coefficient in float format
    
    coefficients= np.array(coefficients.collect()) ## get the coefficients in an array
    
    dictionary = sc.textFile(sys.argv[2], 1) ## read the dictionary input file, to have words and position to get the coefficients
    ### assigned to the right words
    
    dictionary= dictionary.map(lambda x: x.split(',')).map(lambda x: (x[0],int(x[1]))) ## get in the format we need
    
    data_raw = sc.textFile(sys.argv[3], 1) ## read the test input file
    
    ##formating
    d_keyAndText = data_raw.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6]))
    regex = re.compile('[^a-zA-Z]')
    d_keyAndListOfWords = d_keyAndText.map(lambda x : (str(x[0]), regex.sub(' ', x[1]).lower().split()))
    
    allWords = d_keyAndListOfWords.flatMap(lambda p: ((x,1) for x in p[1])) ## all the words in the documents
    allWordsWithDocID = d_keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1])) ##all words with the document ID
    allDictionaryWords = dictionary.join(allWordsWithDocID) ## dicitonary joined with the requiered position
    
    justDocAndPos = allDictionaryWords.map(lambda p: (p[1][1],p[1][0])) ##  mapped as decired
    
    allDictionaryWordsInEachDoc = justDocAndPos.groupByKey() ## group by document to get an aray
    allDocsAsNumpyArrays = allDictionaryWordsInEachDoc.map(lambda x: (x[0], freqArray(x[1],len(x[1])))) ## get document and frequency array
    
    data= allDocsAsNumpyArrays.map(lambda x: (1 if x[0][:2]== 'AU' else 0,np.append(x[1],1.00))) ## 1 for australian court, 0 other wise, plus the frequency array
    
    data.cache() ## faster perfomance
    results=data.map(lambda x: (x[0], 1 if np.dot(x[1],coefficients) > 0 else 0 )) ## mapped as Y and Y estimated
    
    ## mapped as TP, FP,TN,FN to get F measure
    tp_fp_tn_fn=results.map(lambda x: (1 if x[0]== 1 and x[1] == 1 else 0,\
                          1 if x[0]==0 and x[1] == 1 else 0,\
                          1 if x[0]==0 and x[1]==0 else 0,
                          1 if x[0]==1 and x[1]==0 else 0))\
    .reduce(lambda x,y: (x[0]+y[0],x[1] + y[1],x[2] + y[2], x[3]+y[3]))
    ## apply F measure function
    result = f1_measure(tp_fp_tn_fn)
    print("F1 measure is:", result[0])
    ## save result.
    sc.parallelize(result).repartition(1).saveAsTextFile(sys.argv[4],)
