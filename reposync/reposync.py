#!/usr/bin/env python3

import argparse
import json
import os
import re
import shutil
from collections import defaultdict
from difflib import Differ
from functools import lru_cache
from importlib import resources
from io import BytesIO
from json import JSONDecodeError
from pathlib import Path
from string import Template
from sys import stderr
from typing import Dict, Optional

import jsonschema
import requests
from airtable import Airtable
from github import Github, UnknownObjectException
from github.Issue import Issue
from github.ProjectColumn import ProjectColumn
from jenkins import Jenkins, JenkinsException
from jsonschema import ValidationError
from lxml.etree import canonicalize, parse, tostring
from progress.bar import Bar
from xmldiff.main import diff_texts, diff_files

try:
    from . import templates
except:
    import templates

REPOSYNC_CONFIG = Path.home() / '.config' / 'reposync'
AIRTABLE_TOKEN_FILE = REPOSYNC_CONFIG / 'airtable-token'
AIRTABLE_BASE = 'appb66460atpZjzMq'
GITHUB_TOKEN_FILE = REPOSYNC_CONFIG / 'github-token'
JENKINS_TOKEN_FILE = REPOSYNC_CONFIG / 'jenkins-token'


def pathify(label, segments=False):
    """
      Convert a label into something that can be used in a URI path segment.
    """
    return re.sub(r'-$', '',
                  re.sub(r'-+', '-',
                         re.sub(r'[^\w' + ('/]' if segments else ']'), '-', label)))


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


def get_project_board(github_token, repo_url, project_url):
    if not repo_url.startswith(GITHUB_BASE):
        print(f'Github repo URL not recognised {repo_url}.')
        return None
    g = Github(github_token)
    try:
        repo = g.get_repo(repo_url[len(GITHUB_BASE):])
    except UnknownObjectException:
        print(f'Unknown repo {repo_url[len(GITHUB_BASE):]}')
        return
    org = g.get_organization(repo.organization.login)
    return next((project for project in org.get_projects()
                 if (project_url is not None and project.html_url == project_url) or \
                    (project_url is None and project.name == 'Transformation Pipelines')), None)


def update_github(issue_no, title, source, github_token, repo_url, writeback, rec_id, used_labels, issue_column,
                  todo_column):
    if not repo_url.startswith(GITHUB_BASE):
        print(f'Github repo URL not recognised {repo_url}.')
        return None, None
    elif issue_no is not None and issue_no <= 0:
        print(f'Github issue number not valid: {issue_no}.')
        return None, None
    else:
        g = Github(github_token)
        try:
            repo = g.get_repo(repo_url[len(GITHUB_BASE):])
        except UnknownObjectException:
            print(f'Unknown repo {repo_url[len(GITHUB_BASE):]}')
            return None, None
        if issue_no is None:
            # search through issues for a match against the expected title
            issue = next((i for i in repo.get_issues() if i.title == title), None)
            if issue is None:
                print(f"Need to create new GitHub issue for {title}")
                if writeback:
                    issue = repo.create_issue(
                        title,
                        body=resources.read_text(templates, 'issue_body.md')
                    )
        else:
            issue = repo.get_issue(number=issue_no)
        if issue is None:
            print(f'No GitHub issue for {title}')
            return None, None
        if issue_no is None:
            issue_no = issue.number
        airtable_labels = set(
            [label.name for label in issue.get_labels() if label.name in used_labels])
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
            if issue_no not in issue_column and 'To Do' in stage_labels and issue.state == 'open':
                print(f'Issue {issue_no} is not on project board and should be in To Do column.')
                if writeback:
                    card = todo_column.create_card(content_id=issue.id, content_type='Issue')
                    card.move('top', todo_column)
        return issue.number, issue.html_url


def update_web_pages(root_dir):
    for resource in resources.contents(templates):
        if resource.endswith('.html') or resource.endswith('.js') or resource.endswith('.hbs'):
            if resource == 'index.html' and (root_dir / resource).exists():
                continue
            with resources.path(templates, resource) as file_path:
                shutil.copy(file_path, root_dir / resource)


def canonicalize_jenkins_xml(xml: str) -> str:
    tree = parse(BytesIO(bytearray(xml, 'utf-8')))
    for node_with_plugin in tree.xpath('//node()[@plugin]'):
        plugin_without_version = ''.join(node_with_plugin.get('plugin').split('@')[:-1])
        node_with_plugin.set('plugin', plugin_without_version)
    return canonicalize(tree)


def update_jenkins(base, path, creds, name, writeback, github_home):
    server = Jenkins(base, username=creds['username'], password=creds['token'])
    full_job_name = '/'.join(path) + '/' + name
    if server.job_exists(full_job_name):
        job = server.get_job_info(full_job_name)
    else:
        print(f'Jenkins job {full_job_name} doesn''t exist.')
        job = None

    job_template = Template(resources.read_text(templates, 'jenkins_job.xml'))
    config_xml_string = canonicalize_jenkins_xml(job_template.substitute(github_home=github_home,
                                                git_clone_url=github_home + '.git',
                                                dataset_dir=name))

    if job is None and writeback:
        print(f'Creating new job {full_job_name}')
        try:
            server.create_job(full_job_name, config_xml_string)
        except JenkinsException as e:
            print(f'Failed creating job:\n{e}')
    elif job is not None:
        current_xml_string = canonicalize_jenkins_xml(server.get_job_config(full_job_name))
        diffs = diff_files(BytesIO(bytearray(current_xml_string, encoding='utf-8')),
                           BytesIO(bytearray(config_xml_string, encoding='utf-8')))
        if len(diffs) > 0:
            print(f'Jenkins job {full_job_name} needs update')
            if writeback:
                print(diffs)
                if input(f'Are you sure you want to update configuration for {full_job_name} (y/n) ? ') == 'y':
                    print(f'Updating job configuration for {full_job_name}')
                    try:
                        server.reconfig_job(full_job_name, config_xml_string)
                    except JenkinsException as e:
                        print(f'Failed updating job:\n{e}')


def update_airtable_issue(token, issue_number, issue_url, rec_id):
    source = Airtable(AIRTABLE_BASE, 'Source Data', api_key=token)
    # Turn the Key and new Value into a Dictionary
    update = {
        'GitHub Issue Number': issue_number,
        'GitHub Issue URL': issue_url
    }
    # Update AirTable with the new details using the Table name and record ID
    Airtable.update(source, rec_id, update)


@lru_cache()
def fetch_json(url):
    return requests.get(url).json()


def validate(json_obj, schema_url):
    schema_obj = fetch_json(schema_url)
    jsonschema.validate(instance=json_obj, schema=schema_obj)


def sync():
    parser = argparse.ArgumentParser(description='Create / sync family transformations.')
    parser.add_argument('--family', '-f', help='Datasets family to create/sync')
    parser.add_argument('--github', '-g', help='Update/create related GitHub issues', action='store_true')
    parser.add_argument('--jenkins', '-j', help='Update/create related Jenkins jobs', action='store_true')
    parser.add_argument('--airtable', '-a', help='Update Airtable with GitHub issue number & URL', action='store_true')
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

    sources = {record['id']: record['fields'] for record in
               Airtable(AIRTABLE_BASE, 'Source Data', api_key=airtable_token).get_all()}
    families = {record['id']: record['fields'] for record in Airtable(AIRTABLE_BASE, 'Family', api_key=airtable_token).get_all()}
    producers = {record['id']: record['fields'] for record in
                 Airtable(AIRTABLE_BASE, 'Dataset Producer', api_key=airtable_token).get_all()}
    types = {record['id']: record['fields'] for record in Airtable(AIRTABLE_BASE, 'Type', api_key=airtable_token).get_all()}
    tech_stages = set([stage for source in sources.values() for stage in source.get('Tech Stage', [])])
    ba_stages = set([stage for source in sources.values() for stage in source.get('BA Stage', [])])

    datasets_path = Path('datasets')
    datasets_path.mkdir(exist_ok=True)

    main_info_file = datasets_path / 'info.json'
    if main_info_file.exists():
        with open(main_info_file) as info_file:
            main_info = json.load(info_file)
            validate(main_info,
                     main_info.get('$schema', 'http://gss-cogs.github.io/family-schemas/pipelines-schema.json'))
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

    issue_column: Dict[int, ProjectColumn] = {}
    todo_column: Optional[ProjectColumn] = None
    if 'github' in main_info and github_token is not None:
        trans_board = get_project_board(github_token, main_info['github'], main_info.get('project', None))
        print(trans_board.html_url)
        if trans_board is not None:
            for column in Bar('Fetching board issues').iter(list(trans_board.get_columns())):
                if column.name.lower() == 'to do':
                    todo_column = column
                card_contents = [card.get_content() for card in column.get_cards()]
                for issue in card_contents:
                    if isinstance(issue, Issue) and main_info['github'].endswith(issue.repository.full_name):
                        issue_column[issue.number] = column

    source_dataset_path = {}
    for existing_pipeline in datasets_path.iterdir():
        if existing_pipeline.is_dir():
            dataset_info_path = existing_pipeline / 'info.json'
            if dataset_info_path.exists():
                with open(dataset_info_path) as info_file:
                    try:
                        dataset_info = json.load(info_file)
                        validate(dataset_info, dataset_info.get(
                            '$schema', 'http://gss-cogs.github.io/family-schemas/dataset-schema.json'))
                    except JSONDecodeError as e:
                        print(f'Warning: problem reading {dataset_info_path}:\n{e}')
                        continue
                    except ValidationError as ve:
                        print(f'Error validating {dataset_info_path}:')
                        print(f'  {" / ".join(ve.absolute_path)}: {ve.message}')
                if 'transform' in dataset_info and 'airtable' in dataset_info['transform']:
                    recordIds = dataset_info['transform']['airtable']
                    if type(recordIds) != list:
                        recordIds = [recordIds]
                    for recordId in recordIds:
                        if recordId not in source_dataset_path:
                            source_dataset_path[recordId] = existing_pipeline.name
                        else:
                            print(
                                f'Warning: duplicate record ID {recordId} in {existing_pipeline.name} and {source_dataset_path[recordId]}')

    pipelines = []
    dataset_path_source = defaultdict(list)
    touched_info = set()
    for source_id, source in sources.items():
        if ('Family' in source and family_id in source['Family']) or (source_id in source_dataset_path):
            if 'Producer' in source and len(source['Producer']) == 1:
                producer = pathify(producers[source['Producer'][0]]['Name'])
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
                if dataset_info_path.exists():
                    with open(dataset_info_path) as info_file:
                        dataset_info = json.load(info_file)
                else:
                    dataset_info = {}
                dataset_path_source[dataset_dir].append(source_id)
                update_info(dataset_info, source, producers, families, types,
                            dataset_path_source[dataset_dir], dataset_info_path in touched_info)
                if 'github' in main_info:
                    issue_number, issue_url = update_github(dataset_info.get('transform', {}).get('main_issue', None),
                                                            dataset_dir, source, github_token, main_info['github'],
                                                            args.github,
                                                            source_id, tech_stages, issue_column, todo_column)
                    if issue_number is not None:
                        if 'transform' not in dataset_info:
                            dataset_info['transform'] = {}
                        dataset_info['transform']['main_issue'] = issue_number
                        if source.get('GitHub Issue Number', None) != issue_number or \
                                source.get('GitHub Issue URL', None) != issue_url:
                            print(f'Airtable GitHub link needs update')
                            if args.airtable:
                                update_airtable_issue(airtable_token, issue_number, issue_url, source_id)

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
