# airtable-utils
Various scripts for connecting and using data from AirTable

#########################################################################################################################
airtable-create-folder-and-info-files.py:

  Script connects to Airtable COGS and pulls in data from a number of tables
  You will need your own API key to run the script successfully and have permission to access COGS tables

  The script creates a folder structure based on each row in the main ETL table pulling in other information from 
  several other tables when needed. within each folder an info.json file is created

  This code needs further development as columns, Contact Details and Dimensions within the main ETL table needs 
  further development to ensure a consistant format is used.  
  
#########################################################################################################################
