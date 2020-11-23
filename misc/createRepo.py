#!/usr/bin/env python3

# +
import sys
import createRepoUtils as ut
from gssutils import *
import airTableData as at
import getpass
import datetime

import os
from cryptography.fernet import Fernet
# -

family = sys.argv[1].replace(',', '')
atfam = sys.argv[2]
user = getpass.getuser()

# Name of the file for the Python template
pythonFileName = 'main.py'
# Name of the file for the JSON data
jsonFileName = 'info.json'
# Name of the file for the Turtle (ttl) template
ttlFileName = 'flowchart.ttl'
# Name of the file for the Markdown (md) template
mdFileName = "spec.md"

printStatements = True
try:
    print('Accessing AirTable Data')
    dataTables = at.getAirTableETLRecords()
    try:
        if dataTables[0] == 'F':
            ret = 'Failed'
            print('Failed to access AirTable: ' + dataTables[1])
    except Exception as e:
        print('ok')

    srcDat = dataTables[0]
    famDat = dataTables[1]
    prdDat = dataTables[2]
    tpeDat = dataTables[3]
    pmdDat = dataTables[4]
    ret = 'success'

except Exception as e:
    ret = 'AirTable failure: ' + str(e)

try:
    if ret == 'success':

        print('Setting up Variables....')
        oneBackDrive = f'//Users/{user}/Development/'
        mainDir = f'//Users/{user}/Development/'
        familyDirectory = mainDir + family
        refPath = familyDirectory + '/reference'
        dstPath = familyDirectory + '/datasets'

        # Stage Column folder sub-directories
        #fldrPaths = [dstPath + '/candidate', dstPath + '/to-document', dstPath + '/backlog', dstPath + '/prioritized', dstPath + '/published']

        # Change to the Repo you want to work on
        projDir = os.getcwd()
        os.chdir(familyDirectory)

        #repFleNme = 'RepoUpdateReport-' + str(datetime.datetime.now()) + '.sh'
        #file = open(repFleNme, 'w')
        #file.write(f'cd {dstPath} \n')
        #file.close()

        #print('Creating reference Directory, if it does not exist....')
        # Create the Reference directory
        #print(ut.createReferenceDirectory(refPath, dstPath))

        print('Looping around ETL Source Table........................................................')
        try:
            ind = 1
            for i in srcDat:  # CLASS LIST
                try:
                    # If this ETL is for the Family that has been passed then process it
                    if ut.checkFamily(i['fields']['Family'][0], famDat, atfam):
                        nme1 = i['fields']['Name']
                        try:
                            #stg = i['fields']['BA Stage']
                            stg = i['fields']['Tech Stage']
                        except Exception as e:
                            stg = ['No Stage']

                        # Only do stuff if the ETL is Prioritised
                        if 1 == 1: #stg == 'Prioritized':
                            print(nme1 + ' is prioritised, initialising variables')
                            try:
                                pdr = ut.getProducer(i['fields']['Producer'], prdDat)
                            except Exception as e:
                                pdr = ['N/A']

                            try:
                                dtp = ut.getDataType(i['fields']['Data type'], tpeDat)
                            except Exception as e:
                                dtp = ['N/A']

                            i['fields']['Producer'] = pdr
                            i['fields']['Family'][0] = family
                            i['fields']['Data type'] = dtp

                            nme1 = ut.stripString(pdr[0] + ' ' + nme1)
                            nme = ut.stripString(nme1).replace(',', '').replace('  ', ' ').lstrip().rstrip().replace(' ', '-')
                            nme = nme.replace('--', '-')

                            #print('\tLooking if folder exists - STAGE: ' + stg)
                            retPath = ut.seeIfFolderExists(nme, stg, dstPath)#, repFleNme)

                            # Only create the main.py file if currently DOES NOT exist
                            pyPath = Path(retPath / Path(pythonFileName))
                            #if not pyPath.exists():
                            print('\t\tCreating PYTHON template')
                            ut.createMainPYFile(pyPath, nme1, pythonFileName, jsonFileName)

                            print('\t\tCreating TURTLE and MARKDOWN templates')
                            ut.createttlmdtemplates(retPath, nme1, ttlFileName, mdFileName, family, i, nme, user)

                            # Only create and issue if its STAGE is set to Prioritized
                            # Check if the main issue (OPEN) already exists for this transform, if not then create it
                            # if it does exist then get the issue number and pass to make json file method
                            #print('\t\t\tChecking if GitHub Issue already exists')
                            isNum = 1000000
                            #isNum = ut.checkIfIssueAlreadyExists(family, nme, user)
                            #if isNum == -100:
                                #print('\t\t\t\tCreating GitHub Issue')
                                #isNum = ut.makeGithubIssue(nme, family, stg, user)

                            # Update the Issue number in GitHub for this record in the Source Data Table
                            #try:
                                #print('\t\t\t\t\tUpdating GitHub Issue number in AirTable')
                                #rec = at.updateAirTable('Name', i['fields']['Name'], 'GitHub Issue Number', isNum, 'Source Data', projDir, i['id'])
                            #except Exception as e:
                                #print('AIRTABLE Update Failure: ' + str(e))

                            # You can overwrite the JSON file in case some details have changed like Landing Page
                            jsonPath = Path(retPath, jsonFileName)
                            #if not jsonPath.exists():
                            print('\t\t\t\t\tCreating/Updating JSON file')
                            ut.createInfoJSONFile(retPath, i, isNum, jsonFileName, nme)
                    #break
                except Exception as e:
                    print('============== INNER srcDat loop failure: ' + str(e))

            print('Finished Looping....')

        except Exception as e:
            print('================== OUTER srcDat loop failure: ' + str(e))
    else:
        print('Failed to get data from AirTable!')
except Exception as e:

    print('AirTable Failure: ' + str(e))
