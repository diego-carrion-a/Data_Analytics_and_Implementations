from __future__ import print_function

import sys

from pyspark import SparkContext
import numpy as np
from operator import add

### implementation using rdd
if __name__ == "__main__":
    if len(sys.argv) !=3:
        print("Usage: A1Script <file> <output> ", file=sys.stderr) ### how to use the file on spark-submit
        exit(-1)
    
    sc= SparkContext() 
    raw_data = sc.textFile(sys.argv[1], 1) ## read the input file
    raw_data=raw_data.map(lambda p: p+',1.0')
    data= raw_data.map(lambda x: x.split(','))
    data=data.map(lambda p: [float(s) for s in p]).map(lambda p: (p[0],p[1:]))
    data.cache()
    
    n=data.count()
    
    learningRate= 0.000001 
    numIterations = 400
    prior = 9e9
    current = np.ones(6)/10 
    result = []
    
    for i in range(numIterations):
    
        datac=data.map(lambda p: (p[0],p[1],p[0]-(np.dot(p[1],current))))
        sum_calc=datac.map(lambda p: ((p[2]**2, np.multiply(p[1],p[2]),p[2])))
        
        cost = sum_calc.map(lambda p:p[0]).treeAggregate(0, add, add, 2)/(2*n)
        print ('iteration: ',i, 'Cost: ', np.round(cost,6), 'Variables value:', np.round(current,4))  
        
        if (np.abs(prior-cost)<0.01):
            break
        
        if (prior > cost):
            learningRate = learningRate*1.5
        else:
            learningRate = learningRate*0.5
            
        v_grad = (sum_calc.map(lambda p: p[1]).treeAggregate(0, add, add, 2)/n)*(-1)
       
        current= current - (learningRate * v_grad)
        
        prior=cost
     
    result.append(cost)
    result.append(current)
        
    sc.parallelize(result).repartition(1).saveAsTextFile(sys.argv[2])