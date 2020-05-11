from __future__ import print_function

import sys

from pyspark import SparkContext
import numpy as np

from pyspark import *
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import DoubleType
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit

### implementation using rdd
if __name__ == "__main__":
    if len(sys.argv) !=3:
        print("Usage: A1Script <file> <output> ", file=sys.stderr) ### how to use the file on spark-submit
        exit(-1)
    
    sc= SparkContext() 
    sqlContext = SQLContext(sc)
    raw_data = sc.textFile(sys.argv[1], 1) ## read the input file
    ### getting the data in the format desired to computation
    data= raw_data.map(lambda x: x.split(','))\
    .map(lambda p : (float(p[0]),float(p[1]),float(p[2]),float(p[3])\
                     ,float(p[4]),float(p[5])))
    data = sqlContext.createDataFrame(data, ['y', 'x1','x2','x3','x4','x5']) ## creating the data frame
    data.cache()

    ## insight needed for calculation
    n=data.count()
    learningRate= 0.000001 
    numIterations = 400
    m1= 0.1
    m2= 0.1
    m3= 0.1
    m4= 0.1
    m5= 0.1
    b= 0.1
    prior = 9e9
    
    for i in range(numIterations):
        
        ### first we get our y estimated
        datadf= data.withColumn('predicted', ((data.x1 * m1) + (data.x2 * m2) + (data.x3 * m3) + (data.x4 * m4) +(data.x5 * m5) + b))
        
        ## proceed to get the difference between y and y estimated, squared and multiplied by x
        data_predicted=datadf.withColumn('error', (datadf.y - datadf.predicted)).withColumn('squared_error', (datadf.y - datadf.predicted)**2)\
        .withColumn('x1_error', ((datadf.y - datadf.predicted)*datadf.x1))\
        .withColumn('x2_error', ((datadf.y - datadf.predicted)*datadf.x2))\
        .withColumn('x3_error', ((datadf.y - datadf.predicted)*datadf.x3))\
        .withColumn('x4_error', ((datadf.y - datadf.predicted)*datadf.x4))\
        .withColumn('x5_error', ((datadf.y - datadf.predicted)*datadf.x5))\
        .withColumn('reducer', lit(i))
        ## now instead of using reduced by agregation, we can used the iterator as index to use reduceBy and save some time on the computation
        calc = data_predicted.groupBy('reducer').sum('squared_error','x1_error','x2_error','x3_error','x4_error','x5_error','error').collect()
        
        cost= (calc[0][1])/(2*n)
        mg1= (calc[0][2]) * (-1) / n
        mg2= (calc[0][3]) * (-1) / n
        mg3= (calc[0][4]) * (-1) / n
        mg4= (calc[0][5]) * (-1) / n
        mg5= (calc[0][6]) * (-1) / n
        bg= (calc[0][7]) * (-1) / n
        
        ## change learning rate as bold driver to improve time performance
        if (prior > cost):
            learningRate = learningRate*1.5
        else:
            learningRate = learningRate*0.5
        ### condition to stop the iteration
        if (np.abs(prior-cost)<=0.01):
            print('final cost, m and b saved as text file')
            break
        
        m1 = m1 - learningRate* mg1
        m2 = m2 - learningRate* mg2
        m3 = m3 - learningRate* mg3
        m4 = m4 - learningRate* mg4
        m5 = m5 - learningRate* mg5
        b = b - learningRate* bg
        
        prior=cost
        
        print ('Iteration: ',i ,'Cost= ',np.round(cost,6), 'Beta Working Hrs: ',np.round(m1,4), 'Beta Travel Dist: ',np.round(m2,4),'Beta NumRrides: ',np.round(m3,4),'Beta Toll: ',np.round(m4,4), 'Beta Night R: ',np.round(m5,4),'Beta0: ',np.round(b,4))
    
            
    result = []
    result.append(cost)
    result.append(m1)
    result.append(m2)
    result.append(m3)
    result.append(m4)
    result.append(m5)
    result.append(b)
        
    
    sc.parallelize(result).repartition(1).saveAsTextFile(sys.argv[2])
    