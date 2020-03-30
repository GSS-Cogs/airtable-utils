

from airtable import Airtable
from airtable import airtable
import os


# All Tables are from Base COGS Dataset ETL Records
# Source Data Table
baseKey = 'appb66460atpZjzMq'

srcTblNme = 'Source Data'
famTblNme = 'Family'
prdTblNme = 'Dataset Producer'
tpeTblNme = 'Type'
pmdTblNme = 'PMD Dataset'


def getLoginDetails():
    if 'AIRTABLE_API_KEY' in os.environ:
        key = os.environ['AIRTABLE_API_KEY']
    else:
        key = 'Failed to find an Airtable key!'
    #print('Environment Key: ' + key)
    return key


def getAirTableETLRecords():
    try:
        key = getLoginDetails()
        # Get all the information from AirTable - USE YOUR OWN API KEY HERE
        ########################################################################################################
        srcAirTbl = Airtable(baseKey, srcTblNme, api_key=key)
        famAirTbl = Airtable(baseKey, famTblNme, api_key=key)
        prdAirTbl = Airtable(baseKey, prdTblNme, api_key=key)
        tpeAirTbl = Airtable(baseKey, tpeTblNme, api_key=key)
        pmdAirTbl = Airtable(baseKey, pmdTblNme, api_key=key)
        ########################################################################################################

        # Get all the table data from Airtable
        srcDat = srcAirTbl.get_all()
        famDat = famAirTbl.get_all()
        prdDat = prdAirTbl.get_all()
        tpeDat = tpeAirTbl.get_all()
        pmdDat = pmdAirTbl.get_all()

        # Convert the table data into DataFrame format
        #srcDat = pd.DataFrame.from_records((r['fields'] for r in srcDat))

        # -------------------------------------------------------------------------------------------------------------
        retTbls = [srcDat, famDat, prdDat, tpeDat, pmdDat]
    except Exception as e:
        retTbls = ['F', str(e), '', '', '']

    return retTbls


def updateAirTable(key1, val1, key2, val2, tblNme, pDir, recId):
    try:
        # Change directory to the Project directory but save the old dir and go back to it at the end
        currDir = os.getcwd()
        os.chdir(pDir)
        # Get the AirTable log in details
        key = getLoginDetails()
        # Get an instance of the table you want to update
        arTbl = Airtable(baseKey, tblNme, api_key=key)
        # Turn the Key and new Value into a Dictionary
        dicDat = {key2: val2}
        # Update AirTable with the new details using the Table name and record ID
        Airtable.update(arTbl, recId, dicDat)
        os.chdir(currDir)
        return recId
    except Exception as e:
        os.chdir(currDir)
        return 'Update Failed: ' + str(e)


i = 0
#with open('AirTableEncrypt.txt', "r") as input:
#    for line in input:
#        if i == 0:
#            key1 = line.strip().strip("\n")
##        elif i == 1:
#            key2 = line.strip().strip("\n")
#        i = i + 1##

#    input.close()
#print(decrypt(key2.encode(), key1))
#print(key2)


