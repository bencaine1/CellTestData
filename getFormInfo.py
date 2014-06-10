# -*- coding: utf-8 -*-
"""
Created on Thu May 29 10:00:32 2014

@author: aschwartz
"""
import os
import csv
from datetime import datetime
from os import listdir
from os.path import isfile, join, getmtime
from collections import OrderedDict
import sys


basePath = 'C:\\Users\\bcaine\\Desktop\\Dummy Maccor Data\\Data\\ASCIIfiles\\TestFiles';

#get file list and last updated
#fileList = [f for f in listdir(basePath) if isfile(join(basePath,f))]
#fileListDate = [datetime.fromtimestamp(getmtime(basePath + '\\' + f)).strftime("%Y-%m-%d %H:%M:%S") for f in listdir(basePath) if isfile(join(basePath,f)) ]
fileList = [f for f in listdir(basePath) if isfile(join(basePath,f)) and ("FORM" or "form" or "Form") in f]
fileListDate = [datetime.fromtimestamp(getmtime(basePath + '\\' + f)).strftime("%Y-%m-%d %H:%M:%S") for f in listdir(basePath) if isfile(join(basePath,f)) and ("FORM" or "form" or "Form") in f]
errorFiles=[]

# check last update
for i in range(0,len(fileList)):
    myFile = open(basePath + '\\' + fileList[i], 'rb')
    try:
        dialect = csv.Sniffer().sniff(myFile.read())        
        reader = csv.DictReader(myFile, dialect=dialect,delimiter='\t')
        myFile.seek(0)
        some_list = reader.fieldnames
        procTemp = some_list[3]
        index = procTemp.find('Procedure:')
        index2 = procTemp.find('.000')
        procNm = procTemp[index+11:index2]
        reader = csv.DictReader(myFile, dialect=dialect,delimiter= '\t')
        
        # Find all the end of step record nums
        CCchargeStep = None
        ESlist= []
        chargeCap={}
        dischargeCap={}
        cycle = 1
        halfCycle = None
        
        for row in reader:
            #Full Charge Condition
            if int(row["ES"])==133 and row["State"]=='C':
                CCchargeStep = int(row["Step"])
            if CCchargeStep:
                if int(row["ES"])==132 and int(row["Step"])== CCchargeStep + 1:
                   chargeCap[cycle]=float(row["Amp-hr"])
                   halfCycle=1
            #Full Discharge Condition
            if int(row["ES"])==133 and row["State"]=='D':
                dischargeCap[cycle]=float(row["Amp-hr"])
                if halfCycle==1:
                    halfCycle=None
                    cycle += 1
            #Full Cycle Condition
        print fileList[i], "Charge Cap: ", chargeCap, " Discharge Cap: ", dischargeCap

    except csv.Error, e:
        errorFiles.append(fileList[i])
        continue
        

    finally:
        myFile.close()

print "These files didn't process: ", errorFiles
        
        #for row in spamreader:
            #print ', '.join(row)
