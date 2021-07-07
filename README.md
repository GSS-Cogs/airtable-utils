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

API tokens are needed for interaction with Airtable, GitHub and Jenkins. These
are stored in files in the user's home directory under `~/.config/reposync/`.
If the directory doesn't exist:
```
mkdir -p ~/.config/reposync
```

### Airtable API Token

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
  "token": "<long-api-token>"
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
* admin:org / write:org and read:org 

then add it to the .config folder via:

```
echo 'long-alphanumeric-key' > ~/.config/reposync/github-token
```

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
