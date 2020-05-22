
import shutil
from github import Github
from git import Repo
import json
import re
from gssutils import *


REPOSYNC_CONFIG = Path.home() / '.config' / 'reposync'
AIRTABLE_TOKEN_FILE = REPOSYNC_CONFIG / 'airtable-token'
GITHUB_TOKEN_FILE = REPOSYNC_CONFIG / 'github-token'
#JENKINS_TOKEN_FILE = REPOSYNC_CONFIG / 'jenkins-token'


def createInfoJSONFile(pth, jsonInfo, inx, fleNme, gitNme):
    try:
        # Add the git issue number, record ID and creation date to the dictionary
        jsonDict = jsonInfo['fields']
        jsonDict['Main Issue'] = str(inx)
        jsonDict['GitHub Name'] = gitNme
        try:
            jsonDict['AirTable Record ID'] = jsonInfo['id']
            jsonDict['AirTable creation date'] = jsonInfo['createdTime']
        except Exception as e:
            print('Empty Record ID or Creation Date when adding to Dictionary')
        # Filename is set globally at the top of this script
        with open(pth / fleNme, "w") as output:
            output.write(json.dumps(jsonDict, sort_keys=True, indent=4))
            output.close
    except Exception as e:
        return f'{fleNme} file creation failure: ' + str(e)


# Check if the Family is the one you want, passed param has been changed for testing purposes
def checkFamily(transformFam, airTableFamilies, wantedFam):
    try:
        # ASSUMES THEIR IS ONLY ONE FAMILY ASSOCIATED WITH THIS ETL
        # AND PULLS IN THE FIRST ONE FROM THE LIST (SHOULD ONLY BE ONE THING IN THE LIST)
        # --------------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------------
        # THIS NEEDS TO BE REMOVED ONCE WORKING PROPERLY
        #wantedFam = 'COVID-19'
        # --------------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------------
        ret = False
        for fm in airTableFamilies:
            if fm['fields']['Record ID'] == transformFam:
                if fm['fields']['Name'] == wantedFam:
                    ret = True
                    break
        return ret
    except Exception as e:
        return False


def seeIfFolderExists(tfNme, stage, dsPth):
    try:
        print("CHECKING IF FOLDER EXISTS: " + tfNme + " - " + dsPth)
        # This is the current stage of the ETL
        try:
            pth = dsPth + '/' + str(stage[0]) + '/' + str(stage[1]) + '/' + tfNme
        except Exception as e:
            pth = dsPth + '/' + str(stage[0]) + '/' + tfNme
        pth = Path(pth)

        pathExists = False
        pthCheck = pth
        fldrPaths = ['Hold', 'No Stage', 'To Do', 'To Do/Priority']
        for fs in fldrPaths:
            pthCheck = Path(dsPth + '/' + fs + '/' + tfNme)
            if pthCheck.exists():
                pathExists = True
                break

        if not pathExists:
        #if not pth.exists():
            # Folder DOES NOT exist so create it
            pth.mkdir(exist_ok=True, parents=True)
            print('Folder created: ' + str(pth))
            return pth
        else:
            # Folder exists so check if it has been moved to a different stage
            if pth != pthCheck:
                print('\tSTAGE has changed for ETL ::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print('\t\tFrom: ' + str(pthCheck))
                print('\t\tTo:' + str(pth))
                shutil.move(pthCheck, pth)
                print('\tETL Moved ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')

            return pthCheck
    except Exception as e:
        return 'seeIfFolderExists FAILURE: ' + str(e)

# def seeIfFolderExists(tfNme, stage, dsPth, reportName):
#    try:
#        pth = dsPth + '/' + tfNme
#        pth = Path(pth)

#        if not pth.exists():
#            with open(reportName, "a") as repFle:
#                repFle.write('git add ' + tfNme + '\n')

#        pth.mkdir(exist_ok=True, parents=True)

#        return pth
#    except Exception as e:
#        return 'seeIfFolderExists FAILURE: ' + str(e)


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
        return []


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
        return []


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
        ret = f'# # {fNme.strip()} \n\n'
        ret += 'from gssutils import * \n'
        ret += 'import json \n\n'
        ret += f'''info = json.load(open('{jsNme}')) \n'''
        ret += f'''landingPage = info['landingPage'] \n'''
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


def createttlmdtemplates(pth, fNme, ttlNme, mdNme, fam, jsonInfo, flrNme, user):
    try:
        fam = fam.replace('-AIRTABLE','')
        mdPath = Path(pth / Path(mdNme))
        #print('Creating MARKDOWN template:\n' + str(mdPath))
        try:
            lp = jsonInfo['fields']['Landing Page']
        except Exception as e:
            lp = 'PASTE LANDING PAGE HERE'
            print('JSON: ' + str(e))

        ret = '# COGS Dataset Specification\n\n'
        ret += f'[Family Home](https://gss-cogs.github.io/{fam}/datasets/specmenu.html)\n\n'
        ret += f'[Family Transform Status](https://gss-cogs.github.io/{fam}/datasets/index.html)\n\n'
        ret += f'## {fNme.strip()} \n\n'
        ret += f'[Landing Page]({lp})\n\n'
        ret += f'[Transform Flowchart](https://gss-cogs.github.io/{fam}/datasets/specflowcharts.html?{flrNme}/flowchart.ttl)\n\n'
        ret += '### Dataset One\n\n'
        ret += '#### Output Dataset Name:\n\n' + '\t\tname\n\n'
        ret += '#### Table Structure\n\n' + '\t\tPeriod, Measure Type, Unit, Marker, Value\n\n'
        ret += '#### Sheet: 1\n\n' + '\t\tspec\n\n'
        ret += '#### Sheet: 2\n\n' + '\t\tspec\n\n'
        ret += '##### Footnotes\n\n' + '\t\tfootnotes\n\n'
        ret += '##### DM Notes\n\n' + '\t\tnotes\n\n'

        with open(mdPath, "w") as output:
            output.write(ret)
            output.close
    except Exception as e:
        print('MARKDOWN file creation failure: ' + str(e))

    try:
        ttlPath = Path(pth / Path(ttlNme))
        #print('Creating TURTLE template:\n' + str(ttlPath))
        f = open(f"//Users/{user}/Development/flowcharttemplate.ttl", "r")
        with open(ttlPath, "w") as output:
            output.write(f.read())
            output.close
    except Exception as e:
        print('TURTLE creation failure: ' + str(e))




def makeGithubIssue(title, family, stage, user):
    try:
        if GITHUB_TOKEN_FILE.exists():
            with open(GITHUB_TOKEN_FILE) as tf:
                otp = tf.readline().rstrip('\n')

            g = Github(otp)
            for repo in g.get_user().get_repos():
                if repo.name == family:
                    myRepo = repo
                    break
            #label = myRepo.get_label(stage)
            #issue = myRepo.create_issue(title=title, body="Dataset Transformation/Pipeline creation", labels=[label])
            issue = myRepo.create_issue(title=title, body="Dataset Transformation/Pipeline creation")
            issNum = issue.number
            return issNum
        else:
            print('No TOKEN available to create issue')
            return -1
    except Exception as e:
        print('Error creating issue:' + str(e))
        return -1


def checkIfIssueAlreadyExists(family, issueName, user):
    try:
        if GITHUB_TOKEN_FILE.exists():
            with open(GITHUB_TOKEN_FILE) as tf:
                otp = tf.readline().rstrip('\n')

            g = Github(otp)
            ret = -100
            repo = g.get_repo('GSS-Cogs/' + family)
            open_issues = repo.get_issues(state='open')
            for issue in open_issues:
                print(issue.title + ' - ' + issueName)
                if issue.title == issueName:
                    ret = issue.number
                    break
            return ret
        else:
            print('No TOKEN available to check if issue exists')
            return -100
    except Exception as e:
        print('Error checking issue:' + str(e))
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


def renameAndCloneGitHubFolder(fldr, fam, currDir, user, oneBackDrive):
    try:
        os.rename(fldr, fldr + '_TEMP')
        shutil.move(fldr + '_TEMP', oneBackDrive)
        shutil.rmtree(fldr + '_TEMP')
        print('\t Old folder moved now Cloning')
        gitUrl = f'git@github.com:GSS-Cogs/{fam}.git'
        Repo.clone_from(gitUrl, currDir)
        return gitUrl + ' Repo Cloned'
    except Exception as e:
        return 'Failed renaming and Cloning GitHub Folder: ' + str(e)


def deleteRenamedGitHubFolder(fldr):
    try:
        if os.path.exists(fldr + '_TEMP') and os.path.isdir(fldr + '_TEMP'):
            shutil.rmtree(fldr + '_TEMP')
        print('Deleted temp GitHub Folder')
    except Exception as e:
        return 'Failed to delete GitHub Folder: ' + str(e)


def addCommitPushtoGit(fam):
    #try:
    print('Push to Github')
    r = Repo(f'git@github.com:GSS-Cogs/{fam}.git')
    r.git.add(all=True)
    r.index.commit('******************** Updating GitHub Repository *********************')
    origin = r.remote(name='origin')
    origin.push()
    print('Committed')
    #except Exception as e:
        #print('Failed when adding-commiting-pushing to git: ' + str(e))




