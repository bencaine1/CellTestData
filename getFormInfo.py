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
import pyodbc

class CellCycle:
    def __init__(self, test_req, lot_code, cell_idx, end_cycle_dts, cycle_num, cycle_type, cap_charge, cap_discharge):
        self.test_req = test_req
        self.lot_code = lot_code
        self.cell_idx = cell_idx
        self.end_cycle_dts = end_cycle_dts
        self.cycle_num = cycle_num
        self.cycle_type = cycle_type
        self.cap_charge = cap_charge
        self.cap_discharge = cap_discharge
    def __str__(self):
        s = 'test_req: ' + str(self.test_req) + '\n'
        s += 'lot_code: ' + str(self.lot_code) + '\n'
        s += 'cell_idx: ' + str(self.cell_idx) + '\n'
        s += 'end_cycle_dts: ' + str(self.end_cycle_dts) + '\n'
        s += 'cycle_num: ' + str(self.cycle_num) + '\n'
        s += 'cycle_type: ' + str(self.cycle_type) + '\n'
        s += 'cap_charge: ' + str(self.cap_charge) + '\n'
        s += 'cap_discharge: ' + str(self.cap_discharge) + '\n'
        return s

######### SCRAPE ASCII FILES FOR DATA ##########

basePath = 'C:\\Users\\bcaine\\Desktop\\Dummy Maccor Data\\Data\\ASCIIfiles\\TestFiles';

#get file list and last updated
#fileList = [f for f in listdir(basePath) if isfile(join(basePath,f))]
#fileListDate = [datetime.fromtimestamp(getmtime(basePath + '\\' + f)).strftime("%Y-%m-%d %H:%M:%S") for f in listdir(basePath) if isfile(join(basePath,f)) ]
fileList = [f for f in listdir(basePath) if isfile(join(basePath,f)) and ("FORM" or "form" or "Form") in f]
fileListDate = [datetime.fromtimestamp(getmtime(basePath + '\\' + f)).strftime("%Y-%m-%d %H:%M:%S") for f in listdir(basePath) if isfile(join(basePath,f)) and ("FORM" or "form" or "Form") in f]
errorFiles=[]

cellCycles = []

sys.stdout.write('Working')

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
        # look for test_req and cell_idx in file name
        test_req, cell_idx = None, None
        test_req_match = re.search('_(?P<number>[0-9]{6})_', f)
        if test_req_match:
            test_req = test_req_match.group('number')
        cell_idx_match = re.search('_(?P<number>[0-9]{4})[^0-9]', f)
        if cell_idx_match:
            cell_idx = cell_idx_match.group('number')
        
        # Find all the end of step record nums
        CCchargeStep = None
#        ESlist= []
        chargeCap={}
        dischargeCap={}
        end_cycle_dts = {}
        cycle = 1
        halfCycle = None
        
        # FORM02 should contain cycles 2 and 3
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
                end_cycle_dts[cycle] = str(row["DPt Time"])
                if halfCycle==1:
                    halfCycle=None
                    cycle += 1
            #Full Cycle Condition
        
        for key in chargeCap:
            c = CellCycle(test_req, lot_code, cell_idx, end_cycle_dts[key], key, cycle_type, chargeCap[key], dischargeCap[key])
            cellCycles.append(c)
            
        sys.stdout.write('.')
#        print fileList[i], "Charge Cap: ", chargeCap, " Discharge Cap: ", dischargeCap

    except csv.Error, e:
        errorFiles.append(f)
        continue
        
    finally:
        myFile.close()

for c in cellCycles:
    print c

print "These files didn't process: ", errorFiles

########## ADD TO DB ###########

# connect to db
cnxn_str =    """
Driver={SQL Server Native Client 11.0};
Server=172.16.111.235\SQLEXPRESS;
Database=CellTestData;
UID=sa;
PWD=Welcome!;
"""
cnxn = pyodbc.connect(cnxn_str)
cnxn.autocommit = True
cursor = cnxn.cursor()

# Populate TestRequest table
test_req_list = []
test_req_uid = 1
for c in cellCycles:
    if c.test_req not in test_req_list:
        cursor.execute("""
        insert into TestRequest(testReq_UID, testReq_num)
        values (?,?)
        """, test_req_uid, c.test_req)
        test_req_list.append(c.test_req)
        test_req_uid += 1
# Populate CellAssembly table
lot_code_list = []
cell_assy_uid = 1
for c in cellCycles:
    if c.lot_code not in lot_code_list:
        # determine testReq_UID
        test_req_uid = None
        row = cursor.execute("""
        select testReq_UID from TestRequest
        where testReq_num = ?
        """, c.test_req).fetchone()
        if row:
            test_req_uid = row[0]
        cursor.execute("""
        insert into CellAssembly(cellAssy_UID, lotCode, testReq_UID, cell_index)
        values (?,?,?,?)
        """, cell_assy_uid, c.lot_code, test_req_uid, c.cell_idx)
        lot_code_list.append(c.lot_code)
        cell_assy_uid += 1

# Populate CellCycle table
cell_cycle_uid = 1
for c in cellCycles:
    # determine cellAssy_UID
    # Should be 3 CellCycle rows for each 1 CellAssembly row
    # (if both FORM01 and FORM02 present for a given lot code)
    cell_assy_uid = None
    row = cursor.execute("""
    select cellAssy_UID from CellAssembly
    where lotCode = ?
    """, c.lot_code).fetchone()
    if row:
        cell_assy_uid = row[0]
    cursor.execute("""
    insert into CellCycle(cellCycle_UID, cellAssy_UID, endCycle_dts, cycle_num, cycle_type, capacity_charge, capacity_discharge)
    values (?,?,?,?,?,?,?)
    """, cell_cycle_uid, cell_assy_uid, c.end_cycle_dts, c.cycle_num, c.cycle_type, c.cap_charge, c.cap_discharge)
    cell_cycle_uid += 1


        #for row in spamreader:
            #print ', '.join(row)
