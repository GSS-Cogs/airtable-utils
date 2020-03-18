# airtable-utils
Various scripts for connecting and using data from AirTable

## repo-sync

Keeps a local Git repository in sync with the information from Airtables, while
optionally creating/updating related GitHub issues and creating/updating Jenkins
pipelines.

Install the commandline app using pip:

```
pip install git+https://github.com/GSS-Cogs/airtable-utils.git
```

API tokens are needed for interaction with Airtable, GitHub and Jenkins. These
are stored in files in the user's home directory under `~/.config/reposync/`.
If the directory doesn't exist:
```
mkdir ~/.config/reposync
```

### Airable API Token

Log in to Airtable, then click on the account icon, top right, then select "Account"
from the drop down list. This should take you to https://airtable.com/account

From here, you can create a personal API key, or regenerate one if you need to.
The key itself is a string of alpha-numeric characters which you need to put in
the file `~/.config/reposync/airtable-token`, e.g.:
```
echo 'long-alphanumeric-key' > ~/.config/reposync/airtable-token
```

### Jenkins API Token

Log in to Jenkins, then click your name, top right, to get a drop down menu.
Select "Configure" and in the next page, add a new token, calling it "repo-sync"
for example. This token needs to be written to a file `~/.config/reposync/jenkins-token`
but this time as a JSON structure including your username, so take a note
of the username you use in the URL on this page. Use `vim` or `nano` to edit the
file to look like:
```
{
  "username": "<your-username>",
  "token": "<long-api-token",
}
```

### GitHub personal access token

Log in to GitHub and click on your profile, top right, selecting "Settings" from
the drop-down menu, taking you to https://github.com/settings/profile

Click on "Developer settings", then "Personal access tokens" (taking you to
https://github.com/settings/tokens)

Generate a new token, calling it e.g. "repo-sync" and ensure that the following
permissions are enabled:
* repo / public_repo
* user / read:user



#####################################################

airtable-create-folder-and-info-files.py:

  Script connects to Airtable COGS and pulls in data from a number of tables
  You will need your own API key to run the script successfully and have permission to access COGS tables

  The script creates a folder structure based on each row in the main ETL table pulling in other information from 
  several other tables when needed. within each folder an info.json file is created

  This code needs further development as columns, Contact Details and Dimensions within the main ETL table needs 
  further development to ensure a consistant format is used.  
  
#####################################################
