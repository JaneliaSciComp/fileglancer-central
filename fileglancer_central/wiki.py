#!/usr/bin/env python
"""
Download file share paths from the Janelia wiki and save to the Fileglancer database.
Based on documentation at https://atlassian-python-api.readthedocs.io/confluence.html#get-page-info

To use this script you must create a Personal Access Token and save it into your environment:
https://wikis.janelia.org/plugins/personalaccesstokens/usertokens.action
"""

import os
from io import StringIO
import pandas as pd
import pydantic
from datetime import datetime
from atlassian import Confluence

confluence_url = "https://wikis.janelia.org"
confluence_pat = os.environ.get('CONFLUENCE_TOKEN')
confluence = Confluence(url=confluence_url, token=confluence_pat)


def parse_page(page):
    page_id = int(page['id'])
    path = page['_links']['webui']
    title = page['title']
    body = page['body']['view']['value']
    labels = [l['name'] for l in page['metadata']['labels']['results']]
    ancestors = [p['title'] for p in page['ancestors']]
    createdBy = page['history']['createdBy']['displayName']
    authors = [createdBy]
    if 'lastUpdated' in page['history']:
        lastUpdatedBy = page['history']['lastUpdated']['by']['displayName']
        if lastUpdatedBy != createdBy:
            authors.append(lastUpdatedBy)

    return page_id,path,title,body,labels,ancestors,authors


def get_page(page_id):
    # This only retrieves the createdBy and lastUpdatedBy for history
    page = confluence.get_page_by_id(page_id, status=None, version=None,
            expand="body.view,metadata.labels,ancestors,history,history.lastUpdated")
    return parse_page(page)


def parse_iso_timestamp(timestamp):
    return datetime.fromisoformat(timestamp)


page = confluence.get_page_by_title("SCS", "Lab and Project File Share Paths")
page_id = page['id']
page = confluence.get_page_by_id(page_id, status=None, version=None,
            expand="body.view,metadata.labels,ancestors,history,history.lastUpdated")

if 'lastUpdated' in page['history']:
    lastUpdated = page['history']['lastUpdated']['when']
    lastUpdated = parse_iso_timestamp(lastUpdated)
    print(lastUpdated)

body = page['body']['view']['value']
#print(body)

tables = pd.read_html(StringIO(body))
table = tables[0]

# Fill missing values in the table from above values
for column in table.columns:
    last_valid_value = None
    for index, value in table[column].items():
        if pd.isna(value):
            table.at[index, column] = last_valid_value
        else:
            last_valid_value = value

class FileSharePath(pydantic.BaseModel):
    lab: str
    storage: str
    mac_path: str
    smb_path: str
    linux_path: str
    ad_group: str

file_share_paths = []

for index, row in table.iterrows():
    file_share_path = FileSharePath(
        lab=row[table.columns[0]],
        storage=row[table.columns[1]],
        mac_path=row[table.columns[2]],
        smb_path=row[table.columns[3]],
        linux_path=row[table.columns[4]],
        ad_group=row[table.columns[5]]
    )
    file_share_paths.append(file_share_path)

unique_values_column_1 = table[table.columns[1]].unique()
print("Unique values for column 1:", unique_values_column_1)


import json
print(json.dumps(file_share_paths[0].model_dump(), indent=4))




