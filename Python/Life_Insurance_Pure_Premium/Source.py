# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 20:42:51 2019

@author: diego
"""

import math
import os
import pandas as pd
import numpy as np

os.chdir("C:\\Users\\diego\\OneDrive\\Escritorio\\BU\\Python\\FINAL PROJECT")

###Super Class
class Person:
    
    def __init__(self, Age=20, Gender="Female",):
        self.__Age= int(Age)
        self.__Gender = Gender
    
    def getAge(self):
        return self.__Age
    
    def getGender(self):
        return self.__Gender
       
    def setAge (self, age):
        self.__Age = age
    
    def setGender (self, gender):
        self.__Gender = gender
    

##Subclass
class Life_Insurance(Person):
    
    def __init__(self, Type="Whole", Benefit=10000, Coverage=120):
        
        super().__init__()
        self.__Type = Type
        self.__Benefit = float(Benefit)
        self.__Coverage= int(Coverage)
    
    def getType(self):
        return self.__Type
    
    def getBenefit(self):
        return self.__Benefit
    
    def getCoverage(self):
        return self.__Coverage
     
    def setType(self,type_l_insurance):
        self.__Type = type_l_insurance
    
    def setBenefit(self,amount):
        self.__Benefit= amount

    def setCoverage(self,years):
        self.__Coverage= years

    def Estimate_Life_Insurance(self):
        
        if self.getGender()=="Female":
            A= 0.00007
            B= 0.00003
            C= 1.1 
            Interest= 0.04
            V= 1/(1+Interest)
            Age= []
            P= []

            for i in range (self.getAge(),121):
                P.append(round(math.exp((-1*A)-((B/math.log(C))*(C**i)*((C**1)-1))),7))
                Age.append(i)
    
            dict1= {"Age":Age, "P":P}

            DF= pd.DataFrame(dict1)
            DF['Q']= 1- DF['P']
            
        else:
            A= 0.0024
            B= 0.0000025
            C= 1.128
            Interest= 0.04
            V= 1/(1+Interest)
            
            Age= []
            P= []

            for i in range (self.getAge(),121):
                P.append(round(math.exp((-1*A)-((B/math.log(C))*(C**i)*((C**1)-1))),7))
                Age.append(i)
    
            dict1= {"Age":Age, "P":P}

            DF= pd.DataFrame(dict1)
            DF['Q']= 1- DF['P']
                       

        if self.getType() == "Whole":
            A_x=[]
            
            for i in range(len(DF)):
                A_x.append(np.product(P[0:i])* DF['Q'][i]*(V**(i+1)))
            Whole_life= sum(A_x)
            return round(Whole_life * self.getBenefit(),2)
        
        if   self.getType() == "N term":
            
            A_x_n=[]
            for i in range(0,self.getCoverage()):
                if i > len(DF)-1:
                    j = len(DF)
                    A_x_n.append(np.product(P[0:i])* DF['Q'][j-1]*(V**(i+1)))
                else:
                    A_x_n.append(np.product(P[0:i])* DF['Q'][i]*(V**(i+1)))
            N_term = np.nansum(A_x_n) 
            return round(N_term * self.getBenefit(),2)
    
        if  self.getType() == "N Pure Endowment":
            E_N= []
            for i in range(len(DF)):
                if i+self.getCoverage() > len(DF):
                    j = len(DF)
                    E_N.append((V**self.getCoverage())*np.product(P[i:j]))
                else:
                    j= i+ self.getCoverage()
                    E_N.append((V**self.getCoverage())*np.product(P[i:j]))    
            N_term_pure_endowement= E_N[0]
            return  round(N_term_pure_endowement * self.getBenefit(),2)
    
        if self.getType() == "N Endowment": 
            
            A_x_n=[]
            for i in range(0,self.getCoverage()):
                if i > len(DF)-1:
                    j = len(DF)
                    A_x_n.append(np.product(P[0:i])* DF['Q'][j-1]*(V**(i+1)))
                else:
                    A_x_n.append(np.product(P[0:i])* DF['Q'][i]*(V**(i+1)))
            N_term = round(np.nansum(A_x_n),2) 
            
            E_N= []
            for i in range(len(DF)):
                if i+self.getCoverage() > len(DF):
                    j = len(DF)
                    E_N.append((V**self.getCoverage())*np.product(P[i:j]))
                else:
                    j= i+ self.getCoverage()
                    E_N.append((V**self.getCoverage())*np.product(P[i:j]))    
            N_term_pure_endowement= round(E_N[0],2)
    
            N_Endowement = N_term + N_term_pure_endowement
            return round(N_Endowement * self.getBenefit(),2)
    
    
    
    
    