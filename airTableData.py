

from airtable import Airtable
from airtable import airtable
import pandas as pd
import numpy as np
import os
from cryptography.fernet import Fernet


# All Tables are from Base COGS Dataset ETL Records
# Source Data Table
baseKey = 'appb66460atpZjzMq'

srcTblNme = 'Source Data'
famTblNme = 'Family'
prdTblNme = 'Dataset Producer'
tpeTblNme = 'Type'
pmdTblNme = 'PMD Dataset'


def decrypt(token: bytes, key: bytes) -> bytes:
    return Fernet(key).decrypt(token)


def getLoginDetails():
    # Pull in the encrypted keys from a text file to access AirTable bases
    i = 0
    try:
        with open('AirTableEncrypt.txt', "r") as input:
            for line in input:
                if i == 0:
                    key1 = line.strip().strip("\n")
                elif i == 1:
                    key2 = line.strip().strip("\n")
                i = i + 1

            input.close()
            return [key1, key2]
    except Exception as e:
        return ['fail', str(e)]


def getAirTableETLRecords():
    try:
        keys = getLoginDetails()
        key = str(keys[0])
        encryptedKey = str(keys[1])

        # Get all the information from AirTable - USE YOUR OWN API KEY HERE
        ########################################################################################################
        srcAirTbl = Airtable(baseKey, srcTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
        famAirTbl = Airtable(baseKey, famTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
        prdAirTbl = Airtable(baseKey, prdTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
        tpeAirTbl = Airtable(baseKey, tpeTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
        pmdAirTbl = Airtable(baseKey, pmdTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
        ########################################################################################################

        # -------------------------------------------------------------------------------------------------------------

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
        keys = getLoginDetails()
        key = str(keys[0])
        encryptedKey = str(keys[1])
        # Get an instance of the table you want to update
        arTbl = Airtable(baseKey, tblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
        # Retrieve the Record ID of the record you want to update
        #rec = Airtable.match(arTbl, key1, val1)
        # Turn the Key and new Value into a Dictionary
        dicDat = {key2: val2}
        # Update AirTable with the new details using the Table name and record ID
        Airtable.update(arTbl, recId, dicDat)
        os.chdir(currDir)
        return recId
    except Exception as e:
        os.chdir(currDir)
        return 'Update Failed: ' + str(e)

