import sys
import re
import numpy as np
from operator import add
from pyspark import SparkContext
from pyspark import SparkConf
### given function to get freq array
def freqArray (listOfIndices, numberofwords):
    returnVal = np.zeros (20000)
    for index in listOfIndices:
        returnVal[index] = returnVal[index] + 1
    returnVal = np.divide(returnVal, numberofwords)
    return returnVal

### function to retrieve the index of words with high beta value given the result theta array
def top_index_array(array,number):
    theta_c = np.copy(array)
    result=[]
    for i in range(number):
        index= theta_c.argmax()
        result.append(index)
        theta_c[index]=-999999999
    return result

### function to retrieve the top words with high beta value.
def top_words(array,number):
    indexes = top_index_array(array,number)
    result = dictionary.filter(lambda x: x[1] in indexes).collect()
    return result

if __name__ == "__main__":
    if len(sys.argv) !=5:
        print("Usage: A1Script <file> <output> ", file=sys.stderr) ### how to use the file on spark-submit
        exit(-1)

    sc= SparkContext() 
    data_raw = sc.textFile(sys.argv[1], 1) ## read the input file
    d_keyAndText = data_raw.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6])) ## get article and text
    regex = re.compile('[^a-zA-Z]') 
    d_keyAndListOfWords = d_keyAndText.map(lambda x : (str(x[0]), regex.sub(' ', x[1]).lower().split())) ## only lower case words 
    
    allWords = d_keyAndListOfWords.flatMap(lambda p: ((x,1) for x in p[1])) ## list of words
    
    allCounts= allWords.reduceByKey(add) ## count all the words
    
    topWords= allCounts.top(20000,lambda p: p[1]) ## top 20k words
    topWordsK = sc.parallelize(range(20000)) ## index for 20k words
    dictionary = topWordsK.map (lambda x : (topWords[x][0], x)) ## mapped word, position
    
    dictionary.cache() ## faster performance
    
    allWordsWithDocID = d_keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1])) ## all words with document ID
    
    allDictionaryWords = dictionary.join(allWordsWithDocID) ## join to make arrays
    
    allDictionaryWords.cache() 
    
    justDocAndPos = allDictionaryWords.map(lambda p: (p[1][1],p[1][0])) 
    
    allDictionaryWordsInEachDoc = justDocAndPos.groupByKey()
    
    allDocsAsNumpyArrays = allDictionaryWordsInEachDoc.map(lambda x: (x[0], freqArray(x[1],len(x[1])))) ### doc id, word array with frequency
    
    data= allDocsAsNumpyArrays.map(lambda x: (1 if x[0][:2]== 'AU' else 0,np.append(x[1],1.00))) ### 1 for AU 0 for others
    
    data.cache() ## faster performance
    
    ## sigmoid function
    def sigmoid(z):
        return (1/(1+ np.exp(-z))) 
    
    ### parameters
    theta_new = np.full(len(data.take(1)[0][1]),0.1) ## current theta values 
    theta_old = np.zeros(len(data.take(1)[0][1]))  ## old theta values to compare and stop the GD 
    lmbda =5 ## regularization parameter
    learningRate= 0.00003 ## initial learning rate
    numIterations = 200 ## max number of iterations
    prior = 9e9 ## old  cost function
    
    for i in range(numIterations):
        ### data class, frec array, theta*frec
        calcL= data.map(lambda x: (x[0],x[1],np.dot(x[1],theta_new)))
        
        ### calculate the loss function and the gradient
        results= calcL.map(lambda x: (1,(((-x[0]*x[2])+np.log(1+np.exp(x[2]))),\
                                  ((-x[0]*x[1])+(x[1]* sigmoid(x[2]))))))\
        .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).collect()
        
        
        Regularization = np.sum(theta_new**2) ## regularization for cost function
        loss=results[0][1][0] ## get lost value
        loss_regularized = loss + (lmbda * Regularization) ##cost 
        
        ### print to know that cost is going down
        print('iteration: ',i, 'Cost: ', np.round(loss_regularized,6), 'l2 diff:',np.abs(np.sum(np.subtract(theta_new, theta_old)**2)))
        
        ## condition to stop the gradiend descent
        if (np.abs(np.sum(np.subtract(theta_new, theta_old)**2))<0.0001):
                break
        
        
        calcG=results[0][1][1] ## get gradient
        
        Regularization_penalty = 2*lmbda*theta_new 
        grad = np.array(calcG)+ np.array(Regularization_penalty) ## gradiente descen with regularization penalty
        
        theta_old = theta_new ## actualize theta old
        theta_new =  np.subtract(theta_new, (grad * learningRate)) ## new theta
        prior = loss_regularized ## actualize prior cost

    result = top_words(theta_new,5) ## get the top 5 words with largest coefficient
    
    
    ## save dictionary for correct testing
    ## save coefficient for correct testing
    ## save result as required
    codec= "org.apache.hadoop.io.compress.BZip2Codec"
    
    sc.parallelize(result).repartition(1).saveAsTextFile(sys.argv[2],codec)
    sc.parallelize(theta_new).repartition(1).saveAsTextFile(sys.argv[3],codec)
    dictionary.map(lambda x: "{0},{1}".format(x[0],x[1]))\
    .repartition(1).saveAsTextFile(sys.argv[4],codec)
    
    
