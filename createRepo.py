#!/usr/bin/env python3

import shutil
import sys
from github import Github
import json
import re
import createRepoUtils as ut
from gssutils import *
import airTableData as at

#print(sys.argv[0])  # prints python_script.py
#print(sys.argv[1])  # prints var1
#print(sys.argv[2])  # prints var2

family = sys.argv[1].replace(',', '')
user = sys.argv[2].replace(',', '')

# Name of the file for the Python template
pythonFileName = 'main.py'
# Name of the file for the JSON data
jsonFileName = 'info.json'

try:
    print('Accessing AirTable Data')
    dataTables = at.getAirTableETLRecords()
    srcDat = dataTables[0]
    famDat = dataTables[1]
    prdDat = dataTables[2]
    tpeDat = dataTables[3]
    ret = 'success'
except Exception as e:
    ret = 'AirTable failure: ' + str(e)

try:
    if ret == 'success':
        print('Setting up Variables....')
        familyDirectory = f'//Users/{user}/Development/{family}'
        refPath = familyDirectory + '/reference'
        dstPath = familyDirectory + '/datasets'

        # Stage Column folder sub-directories
        fldrPaths = [dstPath + '/candidate',
                    dstPath + '/to-document',
                    dstPath + '/backlog',
                    dstPath + '/prioritized',
                    dstPath + '/published']

        print('Creating reference Directory, if it does not exist....')
        os.chdir(familyDirectory)
        # Create the Reference directory
        print('\t' + ut.createReferenceDirectory(refPath, dstPath))

        print('Looping around ETL Source Table........................................................')
        try:
            ind = 1
            for i in srcDat:  # CLASS LIST
                try:
                    # If this ETL is for the Family that has been passed then process it
                    if ut.checkFamily(i['fields']['Family'][0], family, famDat):
                        print('\t\tSetting up Name, Stage, Producer(s) and Data Type(s)..........')
                        nme1 = i['fields']['Name']
                        stg = i['fields']['Stage']
                        pdr = ut.getProducer(i['fields']['Producer'], prdDat)
                        dtp = ut.getDataType(i['fields']['Data type'], tpeDat)
                        i['fields']['Producer'] = pdr
                        i['fields']['Family'][0] = family
                        i['fields']['Data type'] = dtp
                        print('\t\tTransform name: ' + nme1)
                        nme1 = ut.stripString(pdr[0] + ' ' + nme1)
                        nme = ut.stripString(nme1).replace(',','').replace('  ',' ').lstrip().rstrip().replace(' ', '-').lower()
                        #if nme == 'sg-housing-statistics-for-scotland---key-information-and-summary-tables':
                            #stg = 'Backlog'

                        retPath = ut.seeIfFolderExists(nme, stg, dstPath, fldrPaths)

                        # Only create the main.py file if currently DOES NOT exist
                        pyPath = Path(retPath / Path(pythonFileName))
                        if not pyPath.exists():
                            ut.createMainPYFile(pyPath, nme1, pythonFileName, jsonFileName)

                        # Only create and issue if its STAGE is set to Prioritized
                        # Check if the main issue (OPEN) already exists for this transform, if not then create it
                        # if it does exist then get the issue number and pass to make json file method
                        isNum = ut.checkIfIssueAlreadyExists(family, nme, user)
                        if (isNum == -100) & (stg == 'Prioritized'):
                            isNum = ut.makeGithubIssue(nme, family, stg, user)

                        ut.findGithubProjects(family, user)

                        # You can overwrite the JSON file in case some details have changed like Landing Page
                        ut.createInfoJSONFile(retPath, i['fields'], isNum, jsonFileName)

                        break
                except Exception as e:
                    print('============================================= INNER srcDat loop failure: ' + str(e))

            print('Finished Looping....')
        except Exception as e:
            print('============================================ OUTER srcDat loop failure: ' + str(e))
except Exception as e:
    print('Main Failure: ' + str(e))
