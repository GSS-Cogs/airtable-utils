#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import os
import re

from airtable import Airtable

TOKEN_FILE = Path.home() / '.config' / 'airtable' / 'token'


def pathify(label):
    """
      Convert a label into something that can be used in a URI path segment.
    """
    return re.sub(r'-$', '',
                  re.sub(r'-+', '-',
                         re.sub(r'[^\w/]', '-', label)))


def update_info(info, source, producers, families, types):
    info['title'] = source.get('Name', '').strip()
    info['publisher'] = producers[source['Producer'][0]]['Full Name'].strip()
    info['description'] = source.get('Description', '').strip()
    info['landingPage'] = source.get('Landing Page', '').strip()
    if 'Route from landing page to data' in source:
        info['datasetNotes'] = source['Route from landing page to data'].splitlines()
    # Todo: contact info
    info['published'] = source.get('Last Published', '').strip()
    info['families'] = [families[ref]['Name'] for ref in source.get('Family', [])]
    if 'extract' not in info:
        info['extract'] = {}
    info['extract']['source'] = ', '.join(types[ref]['Name'] for ref in source.get('Data type', []))
    if 'transform' not in info:
        info['transform'] = {}
    info['sizingNotes'] = source.get('Sizing Notes', '').strip()
    info['notes'] = source.get('Notes', '').strip()


def update_github(issue_no, source):
    pass


def sync():
    parser = argparse.ArgumentParser(description='Create / sync family transformations.')
    parser.add_argument('family', help='Datasets family to create/sync')
    args = parser.parse_args()

    if 'AIRTABLE_API_KEY' in os.environ:
        token = os.environ['AIRTABLE_API_KEY']
    elif TOKEN_FILE.exists():
        with open(TOKEN_FILE) as tf:
            token = tf.readline().rstrip('\n')
    else:
        parser.error(f"""Unable to find Airtable API token. Either use an environment variable, AIRTABLE_API_KEY,
or put the token in the file {TOKEN_FILE}""")

    base = 'appb66460atpZjzMq'

    sources = { record['id']: record['fields'] for record in Airtable(base, 'Source Data', api_key=token).get_all() }
    families = { record['id']: record['fields'] for record in Airtable(base, 'Family', api_key=token).get_all() }
    producers = { record['id']: record['fields'] for record in Airtable(base, 'Dataset Producer', api_key=token).get_all() }
    types = { record['id']: record['fields'] for record in Airtable(base, 'Type', api_key=token).get_all() }

    family_id = next((id for (id, family) in families.items() if family['Name'] == args.family), None)
    if family_id is None:
        family_names = [family['Name'] for family in families.values()]
        family_list = " - " + ",\n - ".join(family_names)
        parser.error(f"Family '{args.family}' doesn't exist, choose from:\n{family_list}")

    datasets_path = Path('datasets')
    datasets_path.mkdir(exist_ok=True)

    main_info_file = datasets_path / 'info.json'
    if main_info_file.exists():
        with open(main_info_file) as info_file:
            main_info = json.load(info_file)
    else:
        main_info = {}
    main_info['family'] = families[family_id]["Name"]

    pipelines = []
    for source_id, source in sources.items():
        if 'Family' in source and family_id in source['Family']:
            if 'Producer' in source and len(source['Producer']) == 1:
                producer = producers[source['Producer'][0]]['Name']
                dataset_dir = f"{producer}-{pathify(source['Name'])}"
                prioritized = 'Stage' in source and source['Stage'] == 'Prioritized'
                if not (datasets_path / dataset_dir).exists() and prioritized:
                    (datasets_path / dataset_dir).mkdir(parents=True)
                dataset_info_path = datasets_path / dataset_dir / 'info.json'
                sync_info = False
                if dataset_info_path.exists():
                    with open(dataset_info_path) as info_file:
                        sync_info = True
                        dataset_info = json.load(info_file)
                else:
                    dataset_info = {}
                update_info(dataset_info, source, producers, families, types)
                if 'main_issue' in dataset_info['transform']:
                    update_github(dataset_info['transform']['main_issue'], source)

                if sync_info or prioritized:
                    pipelines.append(dataset_dir)
                    with open(dataset_info_path, 'w') as info_file:
                        json.dump(dataset_info, info_file, indent=4)

    main_info['pipelines'] = sorted(pipelines)
    with open(main_info_file, 'w') as info_file:
        json.dump(main_info, info_file, indent=4)


if __name__ == "__main__":
    sync()
