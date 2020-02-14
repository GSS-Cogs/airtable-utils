

from airtable import Airtable
import pandas as pd
import numpy as np
from cryptography.fernet import Fernet


def decrypt(token: bytes, key: bytes) -> bytes:
    return Fernet(key).decrypt(token)


def getAirTableETLRecords():
    # -------------------------------------------------------------------------------------------------------------
    # Pull in the encrypted keys from a text file to access AirTable bases
    i = 0
    with open('AirTableEncrypt.txt', "r") as input:
        for line in input:
            if i == 0:
                key = line.strip().strip("\n")
            elif i == 1:
                encryptedKey = line.strip().strip("\n")
            i = i + 1

    input.close()
    # -------------------------------------------------------------------------------------------------------------

    # All Tables are from Base COGS Dataset ETL Records
    # Source Data Table
    baseKey = 'appb66460atpZjzMq'

    srcTblNme = 'Source Data'
    famTblNme = 'Family'
    prdTblNme = 'Dataset Producer'
    tpeTblNme = 'Type'

    # -------------------------------------------------------------------------------------------------------------

    # Get all the information from AirTable - USE YOUR OWN API KEY HERE
    ########################################################################################################
    srcAirTbl = Airtable(baseKey, srcTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
    famAirTbl = Airtable(baseKey, famTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
    prdAirTbl = Airtable(baseKey, prdTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
    tpeAirTbl = Airtable(baseKey, tpeTblNme, api_key=str(decrypt(encryptedKey.encode(), key).decode()))
    ########################################################################################################

    # -------------------------------------------------------------------------------------------------------------

    # Get all the table data from Airtable
    srcDat = srcAirTbl.get_all()
    famDat = famAirTbl.get_all()
    prdDat = prdAirTbl.get_all()
    tpeDat = tpeAirTbl.get_all()

    # Convert the table data into DataFrame format
    #srcDat = pd.DataFrame.from_records((r['fields'] for r in srcDat))
    #famDat = pd.DataFrame.from_records((r['fields'] for r in famDat))
    #prdDat = pd.DataFrame.from_records((r['fields'] for r in prdDat))
    #tpeDat = pd.DataFrame.from_records((r['fields'] for r in tpeDat))

    # -------------------------------------------------------------------------------------------------------------
    retTbls = [srcDat, famDat, prdDat, tpeDat]
    return retTbls
