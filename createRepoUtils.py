
import os
import shutil
from github import Github
import json
import re
from gssutils import *

def createInfoJSONFile(pth, jsonDict, inx, fleNme):
    try:
        print('Creating JSON File')
        # Add the git issue number to the dictionary
        jsonDict['Main Issue'] = str(inx)
        # Filename is set globally at the top of this script
        with open(pth / fleNme, "w") as output:
            output.write(json.dumps(jsonDict, sort_keys=True, indent=4))
            output.close
    except Exception as e:
        return f'{fleNme} file creation failure: ' + str(e)


# Check if the Family is the one you want, passed param has been changed for testing purposes
def checkFamily(transformFam, wantedFam, airTableFamilies):
    try:
        # --------------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------------
        # THIS NEEDS TO BE REMOVED ONCE WORKING PROPERLY
        wantedFam = 'Affordable Housing'
        # --------------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------------
        ret = False
        for fm in airTableFamilies:
            if fm['fields']['Record ID'] == transformFam:
                if fm['fields']['Name'] == wantedFam:
                    print('\tcheckFamily: Found Family: ' + fm['fields']['Name'])
                    ret = True
                    break
        return ret
    except Exception as e:
        return False


# See if folder already exists, if not create it. if it does look to see if its STAGE has changed
# if so move it to the correct folder
def seeIfFolderExists(tfNme, stage, dsPth, fldPths):
    try:
        stage = stripString(stage)
        # This is the current stage of the ETL
        pth = dsPth + '/' + stage.lower().replace(' ', '-') + '/' + tfNme
        pth = Path(pth)
        folderExists = False
        # Test all stage folders to see if it already exists
        for fp in fldPths:
            loPth = fp + '/' + tfNme
            loPth = Path(loPth)
            if loPth.exists():
                folderExists = True
                print(f'\t\t\tPath: {loPth} - Folder already exists')
                break
        # If the folder DOES NOT exist then create it.
        # if it exist check if it needs to be moved to a different stage
        if not folderExists:
            # Folder DOES NOT exist so create it
            pth.mkdir(exist_ok=True, parents=True)
            print('Folder created: ' + str(pth))
            return pth
        else:
            # Folder exists so check if it has been moved to a different stage
            if pth != loPth:
                print('\tSTAGE has changed for ETL ::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print('\t\tFrom: ' + str(loPth))
                print('\t\tTo:' + str(pth))
                shutil.move(loPth, pth)
                print('\tETL Moved ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
            return loPth
    except Exception as e:
        return 'seeIfFolderExists FAILURE: ' + str(e)


def getDataType(datTpe, tpeCde):
    try:
        i = 0
        ret = []
        for i in datTpe:
            for dt in tpeCde:
                if dt['fields']['Record ID'] == i:
                    ret.append(dt['fields']['Name'])
                    break
        return ret
    except Exception as e:
        return datTpe


def getProducer(mainPrd, prods):
    try:
        ret = []
        for mp in mainPrd:
            for rw in prods:
                if rw['fields']['Record ID'] == mp:
                    ret.append(rw['fields']['Name'])
                    break
        return ret
    except Exception as e:
        return mainPrd


def createReferenceDirectory(rp, ds):
    colFleNme = 'columns.csv'
    copFleNme = 'components.csv'

    try:
        # Reference folder
        rp1 = Path(rp)
        rp1.mkdir(exist_ok=True, parents=True)
        # Dataset folder
        ds1 = Path(ds)
        ds1.mkdir(exist_ok=True, parents=True)
        # Codelist folder
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


def stripString(theString):
    # REPLACE WILL NOT WORK IN HERE FOR SOME REASON!!!!!!
    theString = re.sub('\ |\?|\.|\!|\/|\;|\:|/|\)|\(|\]|\[|\{|\}|\|\,', ' ', theString)
    return theString


def createMainPYFile(pth, fNme, pyNme, jsNme):
    try:
        # Filename is set globally at the top of this script
        print(f'Creating {pyNme} File')
        ret = f'# # {fNme.strip()} \n\n'
        ret += 'from gssutils import * \n'
        ret += 'import json \n\n'
        ret += f'''info = json.load(open('{jsNme}')) \n'''
        ret += f'''landingPage = info['Landing Page'] \n'''
        ret += 'landingPage \n\n'
        ret += '# + \n'
        ret += f'#### Add transformation script here #### \n\n'
        ret += '''scraper = Scraper(landingPage) \n'''
        ret += 'scraper.select_dataset(latest=True) \n'
        ret += 'scraper '
        with open(pth, "w") as output:
            output.write(ret)
            output.close
    except Exception as e:
        return f'{pyNme} file creation failure: ' + str(e)


def makeGithubIssue(title, family, stage, user):
    try:
        print('\tmakeGithubIssue: Getting GitHub log in details')
        otp = gitHubDetails(user)
        print('\t\tCreating Issue..............................')
        g = Github(otp)
        for repo in g.get_user().get_repos():
            if repo.name == family:
                print('\t\t\tFound Repository: ' + repo.name)
                myRepo = repo
                break
        label = myRepo.get_label(stage)
        issue = myRepo.create_issue(title=title, body="Dataset Transformation/Pipeline creation", labels=[label])
        issNum = issue.number
        return issNum
    except Exception as e:
        return -1


def checkIfIssueAlreadyExists(family, issueName, user):
    try:
        print('\tcheckIfIssueAlreadyExists: Getting GitHub log in details')
        otp = gitHubDetails(user)
        print('\t\tChecking if an OPEN issue already exists........')
        g = Github(otp)
        ret = -100
        repo = g.get_repo('GSS-Cogs/' + family)
        open_issues = repo.get_issues(state='open')
        for issue in open_issues:
            if issue.title == issueName:
                ret = issue.number
                print(f'\t\t\tMain Issue for {issueName} exists - Number: ' + str(ret))
                break
        return ret
    except Exception as e:
        print('Error checking issues: ' + str(e))
        return -100


def gitHubDetails(user):
    try:
        i = 0
        currDir = os.getcwd()
        os.chdir(f'//Users/{user}/Development/airtable-utils')
        with open('gitHubAccess.txt', "r") as input:
            for line in input:
                if i == 0:
                    toke = line.strip().strip("\n")
                i = i + 1
        input.close()
        os.chdir(currDir)
        return toke
    except Exception as e:
        return 'Failed to get GitHub Token' + str(e)


def findGithubProjects(family, user):
    try:
        print('============= Looking for Github Projects ================')
        otp = gitHubDetails(user)
        g = Github(otp)
        for repo in g.get_user().get_repos():
            if repo.name == family:
                print('\t\t\tFound Repository: ' + repo.name)
                myRepo = repo
                break
        print(myRepo)
        try:
            proj = g.get_project(g)
            with open('//Users/leigh/Development/airtable-utils/paginated.json', "w") as output:
                output.write(json.dumps(proj, sort_keys=True, indent=4))
            output.close
        except Exception as e:
            print('Projects say NO!: ' + str(e))

    except Exception as e:
        print('Failed looking for Projects: ' + str(e))
