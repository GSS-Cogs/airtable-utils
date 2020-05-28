#!/usr/bin/env python3

import argparse
import json
import shutil
from collections import defaultdict
from difflib import Differ
from json import JSONDecodeError
from pathlib import Path
import os
import re
from importlib import resources
from string import Template
from sys import stderr
from lxml.etree import canonicalize

from github import Github, UnknownObjectException
from airtable import Airtable
from jenkins import Jenkins, JenkinsException

from . import templates

REPOSYNC_CONFIG = Path.home() / '.config' / 'reposync'
AIRTABLE_TOKEN_FILE = REPOSYNC_CONFIG / 'airtable-token'
GITHUB_TOKEN_FILE = REPOSYNC_CONFIG / 'github-token'
JENKINS_TOKEN_FILE = REPOSYNC_CONFIG / 'jenkins-token'


def pathify(label):
    """
      Convert a label into something that can be used in a URI path segment.
    """
    return re.sub(r'-$', '',
                  re.sub(r'-+', '-',
                         re.sub(r'[^\w/]', '-', label)))


def update_info(info, source, producers, families, types, source_ids, touched):
    info['title'] = source.get('Name', '').strip()
    producer = producers[source['Producer'][0]]
    if 'Full Name' in producer and producer['Full Name'].strip() != '':
        info['publisher'] = producer['Full Name'].strip()
    else:
        info['publisher'] = producer['Name'].strip()
    info['description'] = source.get('Description', '').strip()
    if 'landingPage' in info and touched:
        pages = set()
        if type(info['landingPage']) == str:
            pages.add(info['landingPage'])
        else:
            pages.update(info['landingPage'])
        pages.add(source.get('Landing Page', '').strip())
        if len(pages) == 1:
            info['landingPage'] = pages.pop()
        else:
            info['landingPage'] = sorted(pages)
    elif 'Landing Page' in source and source.get('Landing Page') != '':
        info['landingPage'] = source.get('Landing Page')
    if 'Route from landing page to data' in source:
        info['datasetNotes'] = source['Route from landing page to data'].splitlines()
    # Todo: contact info
    info['published'] = source.get('Last Published', '').strip()
    info['families'] = [families[ref]['Name'] for ref in source.get('Family', [])]
    if 'extract' not in info:
        info['extract'] = {}
    info['extract']['source'] = ', '.join(types[ref]['Name'] for ref in source.get('Data type', []))
    if 'BA Stage' in source:
        info['extract']['stage'] = source['BA Stage']
    if 'transform' not in info:
        info['transform'] = {}
    if 'Tech Stage' in source:
        info['transform']['stage'] = source['Tech Stage']
    if len(source_ids) == 1:
        info['transform']['airtable'] = source_ids[0]
    else:
        info['transform']['airtable'] = source_ids
    info['sizingNotes'] = source.get('Sizing Notes', '').strip()
    info['notes'] = source.get('Notes', '').strip()


GITHUB_BASE = 'https://github.com/'


def update_github(issue_no, title, source, github_token, repo_url, writeback, rec_id, used_labels):
    if not repo_url.startswith(GITHUB_BASE):
        print(f'Github repo URL not recognised {repo_url}.')
        return
    elif issue_no is not None and issue_no <= 0:
        print(f'Github issue number not valid: {issue_no}.')
        return
    else:
        g = Github(github_token)
        try:
            repo = g.get_repo(repo_url[len(GITHUB_BASE):])
        except UnknownObjectException:
            print(f'Unknown repo {repo_url[len(GITHUB_BASE):]}')
            return
        if issue_no is None:
            # search through issues for a match against the expected title
            issue = next((i for i in repo.get_issues() if i.title == title), None)
            if issue is None:
                print(f"Need to create new GitHub issue for {title}")
                if writeback:
                    issue = repo.create_issue(title)
                    update_airtable_issue_number(issue.number, rec_id)
        else:
            issue = repo.get_issue(number=issue_no)
        if issue is None:
            print(f'No GitHub issue for {title}')
            return None
        airtable_labels = set([label.name for label in issue.get_labels() if label.name.startswith('BA') or (label.name in used_labels)])
        if 'Tech Stage' in source:
            stage_labels = set(source['Tech Stage'])
            to_remove = airtable_labels - stage_labels
            to_add = stage_labels - airtable_labels
            if len(to_remove) > 0:
                print(f'Need to remove "{", ".join(to_remove)}" from issue {issue_no}')
                if writeback:
                    for label in to_remove:
                        issue.remove_from_labels(label)
                        print(f'Removed "{label}".')
            if len(to_add) > 0:
                print(f'Need to add "{", ".join(to_add)}" for issue {issue_no}')
                if writeback:
                    for label in to_add:
                        issue.add_to_labels(label)
                        print(f'Added "{label}".')
        return issue.number


def update_web_pages(root_dir):
    for resource in resources.contents(templates):
        if resource.endswith('.html') or resource.endswith('.js') or resource.endswith('.hbs'):
            with resources.path(templates, resource) as file_path:
                shutil.copy(file_path, root_dir / resource)


def update_jenkins(base, path, creds, name, writeback, github_home):
    server = Jenkins(base, username=creds['username'], password=creds['token'])
    full_job_name = '/'.join(path) + '/' + name
    if server.job_exists(full_job_name):
        job = server.get_job_info(full_job_name)
    else:
        print(f'Jenkins job {full_job_name} doesn''t exist.')
        job = None

    job_template = Template(resources.read_text(templates, 'jenkins_job.xml'))
    config_xml = job_template.substitute(github_home=github_home,
                                         git_clone_url=github_home+'.git',
                                         dataset_dir=name)
    config_xml = canonicalize(config_xml)

    if job is None and writeback:
        print(f'Creating new job {full_job_name}')
        try:
            server.create_job(full_job_name, config_xml)
        except JenkinsException as e:
            print(f'Failed creating job:\n{e}')
    elif job is not None:
        current_xml = canonicalize(server.get_job_config(full_job_name))
        if current_xml != config_xml:
            print(f'Jenkins job {full_job_name} needs update')
            if writeback:
                stderr.writelines(Differ().compare(current_xml.splitlines(keepends=True),
                                                   config_xml.splitlines(keepends=True)))
                if input(f'Are you sure you want to update configuration for {full_job_name} (y/n) ? ') == 'y':
                    print(f'Updating job configuration for {full_job_name}')
                    try:
                        server.reconfig_job(full_job_name, config_xml)
                    except JenkinsException as e:
                        print(f'Failed updating job:\n{e}')


def update_airtable_issue_number(issue_number, rec_id):
    if 'AIRTABLE_API_KEY' in os.environ:
        airtable_token = os.environ['AIRTABLE_API_KEY']
    elif AIRTABLE_TOKEN_FILE.exists():
        with open(AIRTABLE_TOKEN_FILE) as tf:
            airtable_token = tf.readline().rstrip('\n')
    base_key = 'appb66460atpZjzMq'
    air_tbl = Airtable(base_key, 'Source Data', api_key=airtable_token)
    # Turn the Key and new Value into a Dictionary
    dic_dat = {'GitHub Issue Number': issue_number}
    # Update AirTable with the new details using the Table name and record ID
    Airtable.update(air_tbl, rec_id, dic_dat)


def sync():
    parser = argparse.ArgumentParser(description='Create / sync family transformations.')
    parser.add_argument('--family', '-f', help='Datasets family to create/sync')
    parser.add_argument('--github', '-g', help='Update/create related GitHub issues', action='store_true')
    parser.add_argument('--jenkins', '-j', help='Update/create related Jenkins jobs', action='store_true')
    args = parser.parse_args()

    if 'AIRTABLE_API_KEY' in os.environ:
        airtable_token = os.environ['AIRTABLE_API_KEY']
    elif AIRTABLE_TOKEN_FILE.exists():
        with open(AIRTABLE_TOKEN_FILE) as tf:
            airtable_token = tf.readline().rstrip('\n')
    else:
        parser.error(f"""Unable to find Airtable API token. Either use an environment variable, AIRTABLE_API_KEY,
or put the token in the file {AIRTABLE_TOKEN_FILE}""")

    if GITHUB_TOKEN_FILE.exists():
        with open(GITHUB_TOKEN_FILE) as tf:
            github_token = tf.readline().rstrip('\n')
    else:
        github_token = None

    if JENKINS_TOKEN_FILE.exists():
        with open(JENKINS_TOKEN_FILE) as tf:
            jenkins_creds = json.load(tf)
    else:
        jenkins_creds = None

    base = 'appb66460atpZjzMq'

    sources = { record['id']: record['fields'] for record in Airtable(base, 'Source Data', api_key=airtable_token).get_all() }
    families = { record['id']: record['fields'] for record in Airtable(base, 'Family', api_key=airtable_token).get_all() }
    producers = { record['id']: record['fields'] for record in Airtable(base, 'Dataset Producer', api_key=airtable_token).get_all() }
    types = { record['id']: record['fields'] for record in Airtable(base, 'Type', api_key=airtable_token).get_all() }
    tech_stages = set([stage for source in sources.values() for stage in source.get('Tech Stage', [])])
    ba_stages = set([stage for source in sources.values() for stage in source.get('BA Stage', [])])

    datasets_path = Path('datasets')
    datasets_path.mkdir(exist_ok=True)

    main_info_file = datasets_path / 'info.json'
    if main_info_file.exists():
        with open(main_info_file) as info_file:
            main_info = json.load(info_file)
    else:
        main_info = {}

    if args.family is not None:
        family_name = args.family
    elif 'family' in main_info:
        family_name = main_info['family']
    else:
        parser.error('No family argument given and no family found in info.json')

    family_id = next((id for (id, family) in families.items() if family['Name'] == family_name), None)
    if family_id is None:
        parser.error(f'Family "{family_name}" not found.')

    main_info['family'] = families[family_id]["Name"]

    if family_id is None:
        family_names = [family['Name'] for family in families.values()]
        family_list = " - " + ",\n - ".join(family_names)
        parser.error(f"Family '{args.family}' doesn't exist, choose from:\n{family_list}")

    source_dataset_path = {}
    for existing_pipeline in datasets_path.iterdir():
        if existing_pipeline.is_dir():
            dataset_info_path = existing_pipeline / 'info.json'
            if dataset_info_path.exists():
                with open(dataset_info_path) as info_file:
                    try:
                        dataset_info = json.load(info_file)
                    except JSONDecodeError as e:
                        print(f'Warning: problem reading {dataset_info_path}:\n{e}')
                        continue
                if 'transform' in dataset_info and 'airtable' in dataset_info['transform']:
                    recordIds = dataset_info['transform']['airtable']
                    if type(recordIds) != list:
                        recordIds = [recordIds]
                    for recordId in recordIds:
                        if recordId not in source_dataset_path:
                            source_dataset_path[recordId] = existing_pipeline.name
                        else:
                            print(f'Warning: duplicate record ID {recordId} in {existing_pipeline.name} and {source_dataset_path[recordId]}')

    pipelines = []
    dataset_path_source = defaultdict(list)
    touched_info = set()
    for source_id, source in sources.items():
        if ('Family' in source and family_id in source['Family']) or (source_id in source_dataset_path):
            if 'Producer' in source and len(source['Producer']) == 1:
                producer = producers[source['Producer'][0]]['Name']
                if source_id in source_dataset_path:
                    dataset_dir = source_dataset_path[source_id]
                elif 'Name' in source:
                    dataset_dir = f"{producer}-{pathify(source['Name'])}"
                else:
                    print(f'No existing dataset directory for source, and source has no name, so ignoring:\n{source}')
                    continue
                if not (datasets_path / dataset_dir).exists():
                    (datasets_path / dataset_dir).mkdir(parents=True)
                dataset_info_path = datasets_path / dataset_dir / 'info.json'
                sync_info = False
                if dataset_info_path.exists():
                    with open(dataset_info_path) as info_file:
                        sync_info = True
                        dataset_info = json.load(info_file)
                else:
                    dataset_info = {}
                dataset_path_source[dataset_dir].append(source_id)
                update_info(dataset_info, source, producers, families, types,
                            dataset_path_source[dataset_dir], dataset_info_path in touched_info)
                if 'github' in main_info:
                    issue_number = update_github(dataset_info.get('transform', {}).get('main_issue', None),
                                                 dataset_dir, source, github_token, main_info['github'], args.github,
                                                 source_id, tech_stages)
                    if issue_number is not None:
                        if 'transform' not in dataset_info:
                            dataset_info['transform'] = {}
                        dataset_info['transform']['main_issue'] = issue_number

                if sync_info:
                    pipelines.append(dataset_dir)
                    with open(dataset_info_path, 'w') as info_file:
                        json.dump(dataset_info, info_file, indent=4)
                        touched_info.add(dataset_info_path)

                if 'jenkins' in main_info and 'base' in main_info['jenkins'] \
                        and 'path' in main_info['jenkins']:
                    update_jenkins(main_info['jenkins']['base'], main_info['jenkins']['path'], jenkins_creds,
                                   dataset_dir, args.jenkins, main_info.get('github', None))

    main_info['pipelines'] = sorted(set(pipelines))
    with open(main_info_file, 'w') as info_file:
        json.dump(main_info, info_file, indent=4)

    update_web_pages(datasets_path)


if __name__ == "__main__":
    sync()
