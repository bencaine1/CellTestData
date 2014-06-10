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
import re

class CellCycle:
    def __init__(self, cycle_num, cycle_type, cap_charge, cap_discharge, test_req, lot_code, cell_idx):
        self.cycle_num = cycle_num
        self.cycle_type = cycle_type
        self.cap_charge = cap_charge
        self.cap_discharge = cap_discharge
        self.test_req = test_req
        self.lot_code = lot_code
        self.cell_idx = cell_idx
    def __str__(self):
        s = 'cycle_num: ' + str(self.cycle_num) + '\n'
        s += 'cycle_type: ' + str(self.cycle_type) + '\n'
        s += 'cap_charge: ' + str(self.cap_charge) + '\n'
        s += 'cap_discharge: ' + str(self.cap_discharge) + '\n'
        s += 'test_req: ' + str(self.test_req) + '\n'
        s += 'lot_code: ' + str(self.lot_code) + '\n'
        s += 'cell_idx: ' + str(self.cell_idx) + '\n'
        return s

basePath = 'C:\\Users\\bcaine\\Desktop\\Dummy Maccor Data\\Data\\ASCIIfiles\\TestFiles';

#get file list and last updated
#fileList = [f for f in listdir(basePath) if isfile(join(basePath,f))]
#fileListDate = [datetime.fromtimestamp(getmtime(basePath + '\\' + f)).strftime("%Y-%m-%d %H:%M:%S") for f in listdir(basePath) if isfile(join(basePath,f)) ]
fileList = [f for f in listdir(basePath) if isfile(join(basePath,f)) and ("FORM" or "form" or "Form") in f]
fileListDate = [datetime.fromtimestamp(getmtime(basePath + '\\' + f)).strftime("%Y-%m-%d %H:%M:%S") for f in listdir(basePath) if isfile(join(basePath,f)) and ("FORM" or "form" or "Form") in f]
errorFiles=[]

cellCycles = []

# check last update
for f in fileList:
    myFile = open(basePath + '\\' + f, 'rb')
    try:
        dialect = csv.Sniffer().sniff(myFile.read())        
        reader = csv.DictReader(myFile, dialect=dialect,delimiter='\t')
        myFile.seek(0)
        some_list = reader.fieldnames
        procTemp = some_list[3]
        index = procTemp.find('Procedure:')
        index2 = procTemp.find('.000')
        procNm = procTemp[index+11:index2]

        lot_tmp = some_list[4]
        index = lot_tmp.find('Barcode: ')
        lot_code = lot_tmp[index+9:]

        reader = csv.DictReader(myFile, dialect=dialect,delimiter= '\t')

        cycle_type = 'Form'
        test_req, cell_idx = None, None
        test_req_match = re.search('_(?P<number>[0-9]{6})_', f) # test_req: search for 6 numbers flanked by underscores
        if test_req_match:
            test_req = test_req_match.group('number')
        cell_idx_match = re.search('_(?P<number>[0-9]{4})_', f) # cell_idx: search for 4 numbers flanked by underscores
        if cell_idx_match:
            cell_idx = cell_idx_match.group('number')
        
        # Find all the end of step record nums
        CCchargeStep = None
#        ESlist= []
        chargeCap={}
        dischargeCap={}
        cycle = 1
        halfCycle = None
        
        if 'form02' in f.lower():
            cycle += 1
        
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
                    
        if 'form01' in f.lower():
            c1 = CellCycle(1, cycle_type, chargeCap[1], dischargeCap[1], test_req, lot_code, cell_idx)
            cellCycles.append(c1)
        elif 'form02' in f.lower():
            c2 = CellCycle(2, cycle_type, chargeCap[2], dischargeCap[2], test_req, lot_code, cell_idx)
            cellCycles.append(c2)
            c3 = CellCycle(3, cycle_type, chargeCap[3], dischargeCap[3], test_req, lot_code, cell_idx)
            cellCycles.append(c3)
            
#        print fileList[i], "Charge Cap: ", chargeCap, " Discharge Cap: ", dischargeCap

    except csv.Error, e:
        errorFiles.append(f)
        continue
        
    finally:
        myFile.close()

for c in cellCycles:
    print c

print "These files didn't process: ", errorFiles
        
        #for row in spamreader:
            #print ', '.join(row)
