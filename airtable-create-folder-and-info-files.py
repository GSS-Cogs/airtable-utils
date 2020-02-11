# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + {"endofcell": "--"}
"""
This script creates a set of info.json files and associated folders using ONS COGS data on AirTable

To access the Bases in AirTable your own personal API key needs to be passed. To get this key go your account 
overview and generate and copy the API key in the scetion labelled API. My own personal key is held in a txt file
on my computer and has been encrypted, so the code will not be able to access the Base unless you have your
own API key.

You will also need a Table Key for each table you with to use. To access this key go to the table and click HELP
in the top right corner and then API Documentation, a new page will open up. In the address bar copy the setion
of the address starting with app, so the address for table 'Dataset Producer' is:
            
            https://airtable.com/appb66460atpZjzMq/api/docs#curl/introduction
            
Copy the middle section, appb66460atpZjzMq, this is the key for the table and should be used to access its 
data along with the table name.

AirTable Base used:
    COGS Dataset ETL Records
        Tables used:
            Source Data
            Family
            Dataset Producer
            Type

The table 'Source Data' holds most of the information needed but some columns hold a 'Record ID', which can be joined
to the other tables to create a full record set
Joins used:
    Source Data(Producer)  ---> Dataset Producer(Record ID) ['Full Name' and 'Name' columns used]
    Source Data(Family)    ---> Family(Record ID) ['Name' column used]
    Source Data(Data Type) ---> Type(Record ID) ['Name' column used]

The column 'Contact details' needs to be formatted differently to others based on given names, email addresses 
and phone numbers. BAs and DEs need to agree on a format that can be used to structure the output properly
The method: formatContactDetails() has been created for this but is not properly coded yet.

The same goes for the column 'Dimensions', the method formatDimensions() had been created for this but has 
not been coded properly yet.

Once all the tables are pulled in a folder called 'infoFiles' is created and then the 'Source Data' table is 
looped through and a folder and info.json file created for each row within this folder.
Folder structure:
    infoFiles
        --> DfE-a-levels-and-other-16-to-18-results
            --> info.json
        --> HESA-higher-education-staff-data
            --> info.json

The name for each folder is created from the values in the 'Name' columns of the tables 'Dataset Producer' and 
'Source Data' (non alpha numeric characters removed and space replaced with -)
    eg: WG-affordable-housing-provision

A main_issue number is given to each info.json file based on the current value of i within the loop. When creating
th associated issues in Github this number should match up, if not then change the number in the info.json file.
"""

import os
from pprint import pprint
from airtable import Airtable
import pandas as pd
import numpy as np
import os
from cryptography.fernet import Fernet
from pathlib import Path
from gssutils import *


def decrypt(token: bytes, key: bytes) -> bytes:
    return Fernet(key).decrypt(token)


# -------------------------------------------------------------------------------------------------------------
# Pull in the encrypted keys from a text file to access AirTable bases
i = 0
with open('AirTableEncrypt.txt', "r") as input:
    for line in input:
        if (i == 0):
            key = line.strip().strip("\n")
        elif (i == 1):
            encryptedKey = line.strip().strip("\n")
        i = i + 1

input.close()

# -------------------------------------------------------------------------------------------------------------

# All Tables are from Base COGS Dataset ETL Records
# Source Data Table
srcTblKey = 'appb66460atpZjzMq'
srcTblNme = 'Source Data'

# Family Table
famTblKey = 'appb66460atpZjzMq'
famTblNme = 'Family'

# Dataset Producer Table
prdTblKey = 'appb66460atpZjzMq'
prdTblNme = 'Dataset Producer'

# Data Type Table
tpeTblKey = 'appb66460atpZjzMq'
tpeTblNme = 'Type'

# -------------------------------------------------------------------------------------------------------------

# Get all the information from AirTable - USE YOUR OWN API KEY HERE
########################################################################################################
srcAirTbl = Airtable(srcTblKey, srcTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
famAirTbl = Airtable(famTblKey, famTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
prdAirTbl = Airtable(prdTblKey, prdTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
tpeAirTbl = Airtable(tpeTblKey, tpeTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
########################################################################################################

# -------------------------------------------------------------------------------------------------------------

# Get all the table data from Airtable
srcDat = srcAirTbl.get_all()
famDat = famAirTbl.get_all()
prdDat = prdAirTbl.get_all()
tpeDat = tpeAirTbl.get_all()

# Convert the table data into DataFrame format
srcDat = pd.DataFrame.from_records((r['fields'] for r in srcDat))
famDat = pd.DataFrame.from_records((r['fields'] for r in famDat))
prdDat = pd.DataFrame.from_records((r['fields'] for r in prdDat))
tpeDat = pd.DataFrame.from_records((r['fields'] for r in tpeDat))

# -------------------------------------------------------------------------------------------------------------

def getProducer(prdCde, fullOrShort):
    try:
        prdCde = str(prdCde).replace('[','').replace(']','').replace("'","")
        retPrd = prdDat['Full Name'][prdDat['Record ID'] == prdCde].index.values.astype(int)
        if (fullOrShort == 1):
            retPrd = prdDat['Full Name'][retPrd[0]] + ' (' + prdDat['Name'][retPrd[0]] + ')'
        else:
            retPrd = prdDat['Name'][retPrd[0]]
        return retPrd
    except Exception as e:
        return "getProducer: " + str(e)


def getFamily(famCde):
    try:
        famCde = str(famCde).replace('[','').replace(']','').replace("'","")
        retFam = famDat['Name'][famDat['Record ID'] == famCde].index.values.astype(int)
        retFam = famDat['Name'][retFam[0]]
        return retFam
    except Exception as e:
        return "getFamily: " + str(e)


def getDataType(tpeCde):
    try:
        i = 0
        retPrd = ''
        tpeCde = tpeCde.split(',')
        for cdes in tpeCde:
            cdes = cdes.replace('[','').replace(']','').replace("'","").strip()
            indPrd = tpeDat['Name'][tpeDat['Record ID'].str.contains(cdes)].index.values.astype(int)
            i = i + 1
            try:
                retPrd = retPrd + ', ' + tpeDat['Name'][indPrd[0]]
            except:
                retPrd = retPrd
        return retPrd
    except Exception as e:
        return "getFamily: " + str(e)


def cleanTitle(mnTle):
    try:
        mnTle = mnTle.rstrip()
        mnTle = mnTle.replace(' ','-')
        mnTle = ''.join(e for e in mnTle if (e.isalnum()) | (e == '-'))
        mnTle = mnTle.lower()
        return mnTle
    except Exception as e:
        return "cleanTitle: " + str(e)


def formatContactDetails(colNme, cntDtls):
    try:
        #cntsDets = cntDtls.split(',')
        #outDtls = '\t\t"' + colNme + '": [{\n'
        #for cnts in cntsDets:
            #cnts = ' '.join([line.strip() for line in cnts.strip().splitlines()])
            #outDtls += f'\t\t\t\t"{cnts}",'
        #outDtls = outDtls[::-1] + '\n'
        #outDtls += '\n\t\t}],\n'

        outDtls = '\t\t"' + colNme + '": "' + cntDtls + '",\n'

        return outDtls
    except Exception as e:
        return 'formatContactDetails: ' + str(e)


def formatDimensions(dmns):
    try:
        return dmns
    except Exception as e:
        return 'formatDimensions: ' + str(e)


def createTransformTemplate():
    try:
        ret = f'# # {mainTitle.strip()} \n\n'
        ret += 'from gssutils import * \n'
        ret += 'import json \n\n'
        ret += f'''info = json.load(open('{jsonStr}')) \n'''
        ret += f'''landingPage = info['{landingPageStr}'] \n'''
        ret += 'landingPage \n\n'
        ret += '# + \n'
        ret += f'#### Add transformation script here #### \n\n'
        ret += '''scraper = Scraper(landingPage) \n'''
        ret += 'scraper.select_dataset(latest=True) \n'
        ret += 'scraper '

        return ret
    except Exception as eee:
        return 'createTransformTemplate: ' + str(eee)


def createReferenceDirectory(mf, rp):
    colFleNme = 'columns.csv'
    copFleNme = 'components.csv'

    try:
        rp1 = Path(Path(mf) / Path(rp))
        rp1.mkdir(exist_ok=True, parents=True)
        cl = 'codelists'
        refPathCL = Path(rp1 / cl)
        refPathCL.mkdir(exist_ok=True, parents=True)
        if not (rp1 / colFleNme).exists():
            with open(rp1 / colFleNme, "w") as output:
                output.write('title,name,component_attachment,property_template,value_template,datatype,value_transformation,regex,range')
                output.close
        if not (rp1 / copFleNme).exists():
            with open(rp1 / copFleNme, "w") as output:
                output.write('Label,Description,Component Type,Codelist')
                output.close
        return 'Reference directory created'
    except Exception as ee:
        print("Reference directory creation failed: " + str(ee))


# ---------------------------------------------------------------------------------------------------------------
# Get the list of column names for looping purposes
colNmes = list(srcDat)
i = 0
strToUse = True
try:
    # Set up some variables so you only need to change them here
    # -----------------------------------------------------------------------------------------------------------
    mainFldr = 'family-affordable-housing-airtable-test'
    datStPath = 'datasets'
    refPath = 'reference'
    landingPageStr = 'Landing Page'
    jsonStr = 'info.json'
    pythonStr = 'main.py'
    # -----------------------------------------------------------------------------------------------------------
    # Stage Column folder sub-directories
    fldrPath = [Path('candidate'),Path('to-document'),Path('backlog'),Path('prioritized'),Path('published')]
    fldrStr = ['Candidate','To document','Backlog','Prioritized','Published']

    # -----------------------------------------------------------------------------------------------------------

    # Create the main folder directory
    mainFldr = Path(mainFldr)
    mainFldr.mkdir(exist_ok=True, parents=True)

    # Create the Reference directory
    print(createReferenceDirectory(mainFldr, refPath))

    # Loop around each row in the source dataset creating the sub-folders and files as needed
    for label, row in srcDat.iterrows():
        myStr = '{\n'
        try:
            if row['Name'].strip() != '':
                mainTitle = row['Name']
                mainTitle = cleanTitle(mainTitle)
                for cols in colNmes:
                    try:
                        strToUse = True
                        myCol = cols
                        myVal = str(row[myCol])
                        myVal = myVal.replace('\n',' ')
                        if 'Producer' in myCol:
                            mainTitle = getProducer(myVal, 2) + '-' + mainTitle.strip().replace('---','-').replace('--','-')
                            myVal2 = getProducer(myVal, 1)
                            myVal = myVal2
                        elif 'Family' in myCol:
                            myVal = getFamily(myVal)
                        elif 'Data type' in myCol:
                            myVal = getDataType(myVal)
                        elif 'Contact Details' in myCol:
                            myVal = formatContactDetails(myCol, myVal)
                            strToUse = False
                        elif 'Dimensions' in myCol:
                            myVal = formatDimensions(myVal)
                        elif landingPageStr in myCol:
                            myVal = ' '.join([line.strip() for line in myVal.strip().splitlines()])
                            lndPg = myVal
                        else:
                            myVal = myVal
                        if strToUse:
                            myVal = ' '.join([line.strip() for line in myVal.strip().splitlines()])
                            myStr += '\t\t"' + myCol + '": "' + myVal.replace("nan",'').replace('"','').strip('\n') + '",\n'
                        else:
                            myStr += myVal.replace("nan",'')
                    except Exception as e:
                        print(f"Row {i} failed: " + mainTitle)
                # Add the main issue number
                myStr += f'\t\t"transform": {{\n\t\t\t\t"main_issue":{i}\n\t\t}}\n}}'

                # Find which sub-directory this should go in and create a file path
                outP = fldrStr.index(row['Stage'])
                famCde = str(row['Family']).replace('[','').replace(']','').replace("'","")
                famInd = famDat['Name'][famDat['Record ID'] == famCde].index.values.astype(int)
                fam = str(famDat['Name'][famInd[0]]).replace(' ','-').lower()
                infoOut = Path(Path(mainFldr) / Path(fam) / Path(datStPath) / Path(fldrPath[outP]) / Path(mainTitle))
                refOut =  Path(Path(mainFldr) / Path(fam))
                infoOut.mkdir(exist_ok=True, parents=True)

                # Create the Reference directory
                print(createReferenceDirectory(refOut, refPath))

                # Create the JSON file
                with open(infoOut / jsonStr, "w") as output:
                    output.write(myStr)
                    output.close

                # Create the Python script template file if it DOES NOT already exist
                if not (infoOut / pythonStr).exists():
                    with open(infoOut / pythonStr, "w") as output:
                        output.write(createTransformTemplate())
                        output.close

                i = i + 1

        except Exception as e:
            print(f"{i}. Inner loop Error: " + str(e))
except Exception as e:
    print(f"{i}. Outer loop Error: " + str(e))

