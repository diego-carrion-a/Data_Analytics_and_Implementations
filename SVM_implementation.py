import sys
import re
import numpy as np
from operator import add
from pyspark import SparkContext
from pyspark import SparkConf
import time

def freqArray (listOfIndices, numberofwords):
    returnVal = np.zeros (20000)
    for index in listOfIndices:
        returnVal[index] = returnVal[index] + 1
    returnVal = np.divide(returnVal, numberofwords)
    #returnVal = returnVal[returnVal>0] ### modify function to return only freq array with values higher than zero as recomended
    return returnVal

def f1_measure(tupl):
    result=[]
    if (tupl[0]+tupl[1] == 0):
        result.append(0)
    else:
        precision = tupl[0]/(tupl[0]+tupl[1])
        recall = tupl[0]/(tupl[0]+tupl[3])
        if (precision+recall) == 0:
            result.append(0)
        else:
            f = (2 * precision * recall) / (precision+recall)
            result.append(f)
    return result

if __name__ == "__main__":
    if len(sys.argv) !=3:
        print("Usage: A1Script <file> <output> ", file=sys.stderr) ### how to use the file on spark-submit
        exit(-1)
    
    sc= SparkContext() 
    
    start_time_overall = time.time()
    start_time_data = time.time()
    ## training data loading
    data_raw= sc.textFile(sys.argv[1], 1)
    d_keyAndText = data_raw.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6]))
    regex = re.compile('[^a-zA-Z]')
    d_keyAndListOfWords = d_keyAndText.map(lambda x : (str(x[0]), regex.sub(' ', x[1]).lower().split()))
    allWords = d_keyAndListOfWords.flatMap(lambda p: ((x,1) for x in p[1]))
    allCounts= allWords.reduceByKey(add)
    topWords= allCounts.top(20000,lambda p: p[1])
    topWordsK = sc.parallelize(range(20000))
    dictionary = topWordsK.map (lambda x : (topWords[x][0], x))
    allWordsWithDocID = d_keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1]))
    allDictionaryWords = dictionary.join(allWordsWithDocID)
    justDocAndPos = allDictionaryWords.map(lambda p: (p[1][1],p[1][0]))
    allDictionaryWordsInEachDoc = justDocAndPos.groupByKey()
    allDocsAsNumpyArrays = allDictionaryWordsInEachDoc.map(lambda x: (x[0], freqArray(x[1],len(x[1]))))
    data_training= allDocsAsNumpyArrays.map(lambda x: (1 if x[0][:2]== 'AU' else -1,x[1]))
    check_zeros = data_training.map(lambda x: x[1]).reduce(add)
    check_zeros=np.array(check_zeros)
    columns_index =check_zeros.argsort()[-10000:][::-1]
    data_training= data_training.map(lambda x: (x[0], x[1][columns_index]))
    data_training= data_training.map(lambda x: (x[0],np.append(x[1],1.00)))
    data_training.cache()
    
    #### testing data loading
    data_raw = sc.textFile(sys.argv[2], 1)
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
    data_testing= allDocsAsNumpyArrays.map(lambda x: (1 if x[0][:2]== 'AU' else -1,x[1])) ## 1 for australian court, 0 other wise, plus the frequency array
    data_testing= data_testing.map(lambda x: (x[0],x[1][columns_index]))
    data_testing= data_testing.map(lambda x: (x[0],np.append(x[1],1))) 
    data_testing.cache() ## faster perfomance
    finishtime1 = time.time() - start_time_data
    
    n=data_training.count()
    
    w_new = np.zeros(10001)
    w_old = np.zeros(10001)
    learningRate= 0.001
    numIterations = 100
    prior = 9e9
    c=5
    lmbda = 1/(n*c)
    
    
    start_time_training = time.time()
    for i in range(numIterations):
        results= data_training.map(lambda x: (1,(np.max([0,1-x[0]*np.dot(w_new,x[1])]),\
                                    w_new if np.max([0,1-x[0]*np.dot(w_new,x[1])]) == 0 else \
                  np.subtract(w_new , np.multiply(x[0],x[1]))))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).\
        map(lambda x: x[1]).collect()
        cost= (results[0][0]/n) + (np.dot(w_new,w_new)*lmbda/2)
        grad = results[0][1]
        gradient= np.divide(grad,n)+ (lmbda*2)
        
        print('iteration: ',i, 'Cost: ', np.round(cost,6))
        
        if (np.abs(prior-cost)< prior*0.001):
            break
        if (prior > cost):
            learningRate = learningRate*1.5
        else:
            learningRate = learningRate*0.5
        
        
        w_old = w_new
        w_new =  np.subtract(w_new, (gradient * learningRate))
        prior = cost 
    finishtime2=time.time() - start_time_training
    
    start_time_testing = time.time()
    results=data_testing.map(lambda x: (x[0], np.sign(np.dot(x[1],w_new))))
    finishtime3=time.time() - start_time_testing
    
    tp_fp_tn_fn=results.map(lambda x: (1 if x[0]== 1 and x[1] == 1 else 0,\
                          1 if x[0]==-1 and x[1] == 1 else 0,\
                          1 if x[0]==-1 and x[1]== -1 else 0,
                          1 if x[0]==1 and x[1]==-1 else 0))\
    .reduce(lambda x,y: (x[0]+y[0],x[1] + y[1],x[2] + y[2], x[3]+y[3]))
    
    result = f1_measure(tp_fp_tn_fn)
    finishtime4=time.time() - start_time_overall
    
    print("F1 measure is:",  result[0])
    print("Reading an preparing Data Time","- %s seconds -" % np.round(finishtime1,2))
    print("Training Time","- %s seconds -" % np.round(finishtime2,2))
    print("Testing Time","- %s seconds -" % np.round(finishtime3,2))
    print("Over all Time","- %s seconds -" % np.round(finishtime4,2))
