# airtable-utils
Various scripts for connecting and using data from AirTable

## repo-sync

Keeps a local Git repository in sync with the information from Airtables, while
optionally creating/updating related GitHub issues and creating/updating Jenkins
pipelines.

## Installation
If you're using a `databaker-docker` container, `airtable-utils` is already installed and ready to use. Skip the manual installation step and go straight to Airtable API Token.

### Manual Installation
Install the commandline app using pip:

```
pip install git+https://github.com/GSS-Cogs/airtable-utils.git
```

## Creating the reposync sub-directory
API tokens are needed for interaction with Airtable, GitHub and Jenkins. These
are stored in the respective API token files in the reposync sub-directory in the .config directory in the user's home directory:`~/.config/reposync/`. 
For example the Airtable API token file would be stored in `airtable-token` file in `~/.config/reposync/` 
If the config directory and reposync sub-directory are not in the user's home directory, they could be created with the make directory command:
```
mkdir -p ~/.config/reposync
```

### Airtable API Token

Log in to Airtable, then click on the account icon, top right, then select "Account"
from the drop down list. This takes the user to https://airtable.com/account

From here,  create a personal API key, or regenerate one if need be.
The key itself is a string of alpha-numeric characters which you need to put in airtable-token file. 
The way to create the file is to change from the root directory to the home directory and then .config directory anf then reposync directory
 `cd ~/.config/reposync/`.

Then create the air-table token file with `touch airtable token`.
Call up or edit the file using nano `nano airtable token` and write the Airtable API token to the file. If nano is not installed on your directory, use `apt install nano` to install.

Make a note of the airtabe-token as it would be required if the docker container is deleted (containers don't last forever)

### Jenkins API Token

Log in to Jenkins, then click your name, top right, to get a drop down menu.
Select "Configure" and in the next page, add a new token, calling it "repo-sync" for example. 
Still in the reposync sub-directory `~/.config/reposync/`(as explained with Airtable API token), create the jenkins-token file with `touch jenkins-token`. Call up or edit the file `nano jenkins-token` and write the Jenkins API token to the file as a JSON structure including your username. Make a note of the username you in the URL on this page. 

The jenkins-token file should look like this with the username and token given as `strings`:
```
{
  "username": "<your-username>",
  "token": "<long-api-token>"
}
```

Make a note of the jenkins-token as it would be required if the docker container is deleted.

### GitHub personal access token

Log in to GitHub and click on your profile, top right, selecting "Settings" from
the drop-down menu, taking you to https://github.com/settings/profile

Click on "Developer settings", then "Personal access tokens" (taking you to
https://github.com/settings/tokens)

Generate a new token, calling it e.g. "repo-sync" and ensure that the following
permissions are enabled:
* repo / public_repo
* user / read:user
* admin:org / write:org and read:org 

Still in the reposync sub-directory `~/.config/reposync/`(as explained in Airtable API token), create the github-token file with `touch github-token`. Call up or edit the file `nano github-token` and write the GitHUb personal access token to the github-token file.

Make a note of the github-token as it would be required if the docker container is deleted.

### Running repo-sync

In an already cloned `family-*` repository. *First create a new branch so that you can pick the changes you want to merge.* The command is:

```
repo-sync --family "Family Name"
```
to pull in the datasets from Airtable and create all necessary directories and info.json files. This is the first command to be run when setting up a new family repository. Note replace 'Family Name' with correct name e.g. `repo-sync --family "Climate Change"`.

```
repo-sync
```
with no arguments, all the configuration will be picked up from the
`datasets/info.json` file. Any new or updated information from Airtable
for this dataset family will be added/updated in local files, which can
then be checked in the usual way before committing changes and pushing
them back with `git`.

If there are any notes about GitHub issues, re-running with
```
repo-sync -g
```
will make changes to the GitHub issues, updating existing labels or adding
new issues as required, moving the issues to the right place in the project
board if they are prioritized.

If there are any notes about Jenkins jobs, re-running with
```
repo-sync -j
```
will create new Jenkins jobs automatically and will ask for confirmation
before changing any existing jobs.

This last point, about changing existing jobs, should generally be answered
with "n" since it's difficult to work out whether a job definition has
changed or whether it's just the various plugin versions that have been
updated.

If there are any notes about Airtable, re-running with 
```
repo-sync -a
```
will update Airtable with any GitHub issue numbers or URLs.
