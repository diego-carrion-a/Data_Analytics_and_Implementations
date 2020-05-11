# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 11:22:30 2019

@author: diego
"""
import os
os.chdir("C:\\Users\\diego\\OneDrive\\Escritorio\\BU\\Python\\FINAL PROJECT")

from Source import Person 
from Source import Life_Insurance
import tkinter as tk

fields = ('Age', 'Gender', 'Type', 'Benefit', 'Years of Coverage', 'Risk Premium')

def calculation(entries):
    # age:
    age = int(entries['Age'].get()) 
    print("Age", age)
    # gender:
    gender = entries['Gender'].get()
    Type_of_insurance = entries['Type'].get()
    Benefit =  float(entries['Benefit'].get())
    Coverage = int(entries['Years of Coverage'].get())
    P = Person(age,gender)
    I1 = Life_Insurance(Type_of_insurance,Benefit,Coverage)
    I1.setAge(P.getAge())
    I1.setGender(P.getGender())
    Risk_premium = I1.Estimate_Life_Insurance() 
    entries['Risk Premium'].delete(0, tk.END)
    entries['Risk Premium'].insert(0, Risk_premium )
    print("Risk Premium: %f" % float(Risk_premium))

def makeform(root, fields):
    entries = {}
    for field in fields:
        print(field)
        row = tk.Frame(root)
        lab = tk.Label(row, width=22, text=field+": ", anchor='w')
        ent = tk.Entry(row)
        ent.insert(0, "0")
        row.pack(side=tk.TOP, 
                 fill=tk.X, 
                 padx=5, 
                 pady=5)
        lab.pack(side=tk.LEFT)
        ent.pack(side=tk.RIGHT, 
                 expand=tk.YES, 
                 fill=tk.X)
        entries[field] = ent
    return entries

if __name__ == '__main__':
    root = tk.Tk()
    ents = makeform(root, fields)
    b1 = tk.Button(root, text='Risk Premium',
           command=(lambda e=ents: calculation(e)))
    b1.pack(side=tk.LEFT, padx=5, pady=5)
    b3 = tk.Button(root, text='Quit', command=root.quit)
    b3.pack(side=tk.LEFT, padx=5, pady=5)
    
    root2 = tk.Tk()
    T2 = tk.Text(root2, height=30, width=150)
    T2.pack()
    T2.insert(tk.END, "Welcome User\nPlease read the following instructions\
         \nPlease insert your age, gender, Type of insurance, Benefit and Years of coverage\
         \nIn case you are not familiar with the terms, there are 4 types of Life insurance avaliable in this program:\
         \n  Whole life insurance    --> Covers against death during the remain years of life, the input code is 'Whole'\
         \n  N Term of insurance     --> Covers against death during a selected years of coverage, the input code is 'N term'\
         \n  N Years Pure Endowement --> Pays the beneficiary in case of surviving a certaing number of yeras, the input code is 'N Pure Endowment'\
         \n  N Years Endowement      --> Pays the beneficiary in case of death or surviving a selected years of coverage, the input code is 'N Endowment'\
         \n\
         \nBenefit is the amount to be paid in case of death or surviving.\
         \nYears of coverage only needed when you want to estimate a different insurance than Whole life insurance\
         \nAfter introducing the correct code of the type of insurance you want to calculate, hit the 'Risk Premium' button.\
         \nThe number appearing in the Risk Premium box is the minimal amount to be charged for that type of insurance, with the characteristics specified.")

    
    root.mainloop()
