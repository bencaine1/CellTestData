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

# connect to db
cnxn_str = """
Driver={SQL Server Native Client 11.0};
Server=172.16.111.235\SQLEXPRESS;
Database=CellTestData;
UID=sa;
PWD=Welcome!;
"""
cnxn = pyodbc.connect(cnxn_str)
cnxn.autocommit = True
cursor = cnxn.cursor()

basePath = '\\\\24m-fp01\\24m\\MasterData\\Battery_Tester_Backup\\24MBattTester_Maccor\\Data\\ASCIIfiles';
#basePath = 'C:\\Users\\bcaine\\Desktop\\Dummy Maccor Data\\data\\ASCIIfiles';

errorFiles = []

cellCycles = []

sys.stdout.write('Working')

# search folders and subfolders
for dirpath, dirnames, filenames in os.walk(basePath):
    for f in filenames:
        if ("form") in f.lower():
            # check last update, skip if already in FileUpdate db
            date = datetime.fromtimestamp(getmtime(os.path.join(dirpath, f))).strftime("%Y-%m-%d %H:%M:%S")
            row = cursor.execute("""
            select * from FileUpdate
            where Filename = ? and LastUpdate = ?;
            """, f, date).fetchone()
            if row:
                sys.stdout.write('^')
                continue

            # look for test req in file name, skip if not present
            test_req = None
            test_req_match = re.search('_(?P<number>[0-9]{6})_', f)
            if test_req_match:
                test_req = test_req_match.group('number')
            else:
                sys.stdout.write(',')
                continue

            myFile = open(os.path.join(dirpath, f), 'rb')
            try:
                dialect = csv.Sniffer().sniff(myFile.read())        
                reader = csv.DictReader(myFile, dialect=dialect,delimiter='\t')
                myFile.seek(0)
                some_list = reader.fieldnames
                procTemp = some_list[3]
                index = procTemp.find('Procedure:')
                index2 = procTemp.find('.000')
                procNm = procTemp[index+11:index2]
                
                # get lot code from E1, skip if not present
                try:
                    lot_tmp = some_list[4]
                    index = lot_tmp.find('Barcode: ')
                    lot_code = lot_tmp[index+9:]
                except:
                    sys.stdout.write('l')
                    myFile.close()
                    continue
                            
                reader = csv.DictReader(myFile, dialect=dialect,delimiter= '\t')

                cell_idx = None
                cycle_type = 'Form'
                cell_idx_match = re.search('_(?P<number>[0-9]{4})[^0-9]', f)
                if cell_idx_match:
                    cell_idx = cell_idx_match.group('number')
                
                # Find all the end of step record nums
                CCchargeStep = None
#                ESlist= []
                chargeCap={}
                dischargeCap={}
                end_cycle_dts = {}
                cycle = 1
                halfCycle = None
                
                # FORM02 should contain cycles 2 and 3
                if ('form02' or 'form2') in f.lower():
                    cycle += 1
                
                full_cycle = False
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
                    if int(row["ES"])==193 and row["State"]=='O':
                        full_cycle = True
                
                if full_cycle:
                    for key in chargeCap:
                        try:
                            c = CellCycle(test_req, lot_code, cell_idx, end_cycle_dts[key], key, cycle_type, chargeCap[key], dischargeCap[key])
                            cellCycles.append(c)
                        except KeyError:
                            print 'Key Error in ', f
                            continue
                
                # All ok, so add row to FileUpdate table
                cursor.execute("""
                merge FileUpdate as T
                using (select ?, ?) as S (Filename, LastUpdate)
                on S.Filename = T.Filename and S.LastUpdate = T.LastUpdate
                when not matched then insert(Filename, LastUpdate)
                values (S.Filename, S.LastUpdate);
                """, f, date)
                sys.stdout.write('.')
        
            except csv.Error, e:
                errorFiles.append(f)
                continue
                
            finally:
                myFile.close()
#for c in cellCycles:
#    print c

print "\nThese files didn't process: ", errorFiles

########## ADD TO DB ###########

# Delete tables if 'delete' passed in as arg.
if len(sys.argv) > 1 and sys.argv[1] == 'delete':
    cursor.execute("""
    delete from CellCycle;
    delete from CellAssembly;
    delete from TestRequest;    
    """)

# Populate TestRequest table
print 'Populating TestRequest table...'
test_req_list = []
for c in cellCycles:
    if c.test_req not in test_req_list:
        cursor.execute("""
        merge TestRequest as T
        using (select ?) as S (testReq_num)
        on S.testReq_num = T.testReq_num
        when not matched then insert(testReq_num)
        values (S.testReq_num);
        """, c.test_req)
        test_req_list.append(c.test_req)
        
# Populate CellAssembly table
print 'Populating CellAssembly table...'
lot_code_list = []
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
        # Merge on same test req UID and cell index
        cursor.execute("""
        merge CellAssembly as T
        using (select ?, ?, ?) as S (lotCode, testReq_UID, cell_index)
        on S.testReq_UID = T.testReq_UID and S.cell_index = T.cell_index
        when not matched then insert(lotCode, testReq_UID, cell_index)
        values (S.lotCode, S.testReq_UID, S.cell_index);
        """, c.lot_code, test_req_uid, c.cell_idx)
        lot_code_list.append(c.lot_code)

# Populate CellCycle table
print 'Populating CellCycle table...'
for c in cellCycles:
    # determine cellAssy_UID
    # Should be 3 CellCycle rows for each 1 CellAssembly row,
    # if both FORM01 and FORM02 present for a given lot code.
    cell_assy_uid = None
    row = cursor.execute("""
    select cellAssy_UID from CellAssembly
    where lotCode = ?
    """, c.lot_code).fetchone()
    if row:
        cell_assy_uid = row[0]
    cursor.execute("""
    merge CellCycle as T
    using (select ?, ?, ?, ?, ?, ?) as S (cellAssy_UID, endCycle_dts, cycle_num, cycle_type, capacity_charge, capacity_discharge)
    on S.cellAssy_UID = T.cellAssy_UID and S.cycle_num = T.cycle_num
    when not matched then insert(cellAssy_UID, endCycle_dts, cycle_num, cycle_type, capacity_charge, capacity_discharge)
    values (S.cellAssy_UID, S.endCycle_dts, S.cycle_num, S.cycle_type, S.capacity_charge, S.capacity_discharge);
    """, cell_assy_uid, c.end_cycle_dts, c.cycle_num, c.cycle_type, c.cap_charge, c.cap_discharge)

#close up shop
cursor.close()
del cursor
cnxn.close()
