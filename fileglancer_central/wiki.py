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
from datetime import datetime
from atlassian import Confluence
from .database import get_db_session, update_file_share_paths
from .settings import get_settings
from loguru import logger
settings = get_settings()


def parse_iso_timestamp(timestamp):
    """Parse ISO format timestamp string to datetime object"""
    return datetime.fromisoformat(timestamp)


def get_wiki_table():
    """Fetch and parse the file share paths table from the wiki"""
    confluence_url = settings.confluence_url
    confluence_pat = settings.confluence_token
    confluence = Confluence(url=str(confluence_url), token=confluence_pat)
    
    page = confluence.get_page_by_title("SCS", "Lab and Project File Share Paths")
    page_id = page['id']
    page = confluence.get_page_by_id(page_id, status=None, version=None,
                expand="body.view,metadata.labels,ancestors,history,history.lastUpdated")

    if 'lastUpdated' in page['history']:
        lastUpdated = page['history']['lastUpdated']['when']
        lastUpdated = parse_iso_timestamp(lastUpdated)
        print(lastUpdated)

    body = page['body']['view']['value']
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
    
    logger.debug(f"Found {len(table)} file share paths in the wiki")  
    return table


def refresh_paths():
    """Refresh the file share paths from the wiki"""
    session = get_db_session()
    table = get_wiki_table()
    update_file_share_paths(session, table)


if __name__ == "__main__":
    refresh_paths()
