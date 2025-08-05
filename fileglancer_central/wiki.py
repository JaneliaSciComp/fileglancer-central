#!/usr/bin/env python
"""
Download file share paths from the Janelia wiki and save to the Fileglancer database.
Based on documentation at https://atlassian-python-api.readthedocs.io/confluence.html#get-page-info

To use this script you must create a Personal Access Token and save it into your environment:
https://wikis.janelia.org/plugins/personalaccesstokens/usertokens.action
"""

from io import StringIO
import pandas as pd
from datetime import datetime
from atlassian import Confluence
from .settings import get_settings
from loguru import logger

from fileglancer_central.database import FileSharePathDB, ExternalBucketDB
from fileglancer_central.utils import slugify_path

settings = get_settings()


def parse_iso_timestamp(timestamp):
    """Parse ISO format timestamp string to datetime object"""
    return datetime.fromisoformat(timestamp)


def get_confluence_client() -> Confluence:
    confluence_server = str(settings.atlassian_url)
    confluence_username = settings.atlassian_username
    confluence_token = settings.atlassian_token
    return Confluence(url=confluence_server, username=confluence_username, password=confluence_token, cloud=True)


def get_file_share_paths_table():
    """Fetch and parse the file share paths table from the wiki"""
    confluence = get_confluence_client()

    confluence_space = "SCS"
    confluence_page = "Lab and Project File Share Paths"

    page = confluence.get_page_by_title(confluence_space, confluence_page)
    if not page:
        raise ValueError(f"Could not find page {confluence_page} in space {confluence_space}")

    page = confluence.get_page_by_id(page['id'], status=None, version=None,
                expand="body.view,metadata.labels,ancestors,history,history.lastUpdated")

    # Get the last updated timestamp for the table
    table_last_updated = None
    if 'lastUpdated' in page['history']:
        table_last_updated = parse_iso_timestamp(page['history']['lastUpdated']['when'])

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
    
    column_names = ('lab', 'storage', 'mac_path', 'windows_path', 'linux_path', 'group')
    table.columns = column_names

    logger.debug(f"Found {len(table)} file share paths in the wiki")  
    return table, table_last_updated


def convert_table_to_file_share_paths(table):
    """Convert the wiki table to a list of DB objects"""
    return [FileSharePathDB(
        name=slugify_path(row.linux_path),
        zone=row.lab,
        group=row.group,
        storage=row.storage,
        mount_path=row.linux_path,
        mac_path=row.mac_path,
        windows_path=row.windows_path,
        linux_path=row.linux_path,
    ) for row in table.itertuples(index=False)]


def get_external_buckets():
    """Fetch and parse the external buckets table from the wiki"""
    confluence = get_confluence_client()

    confluence_space = "ScientificComputing"
    confluence_page = "S3 Buckets on Janelia Shared Storage"

    page = confluence.get_page_by_title(confluence_space, confluence_page)
    if not page:
        raise ValueError(f"Could not find page {confluence_page} in space {confluence_space}")

    page = confluence.get_page_by_id(page['id'], status=None, version=None,
                expand="body.view,metadata.labels,ancestors,history,history.lastUpdated")

    # Get the last updated timestamp for the table
    table_last_updated = None
    if 'lastUpdated' in page['history']:
        table_last_updated = parse_iso_timestamp(page['history']['lastUpdated']['when'])

    body = page['body']['view']['value']
    tables = pd.read_html(StringIO(body))
    
    # Combine all tables that contain external bucket data
    all_buckets = []
    
    for i, table in enumerate(tables):
        logger.debug(f"Processing table {i+1} with columns: {list(table.columns)}")
        
        # Check if this table has the expected columns
        if 'External URL' not in table.columns or 'Filesystem Path' not in table.columns:
            logger.debug(f"Skipping table {i+1} - missing required columns")
            continue
            
        # Extract relevant columns
        table_subset = table[['External URL', 'Filesystem Path']]
        
        # Convert to ExternalBucketDB objects
        for _, row in table_subset.iterrows():
            full_path = row['Filesystem Path']
            external_url = row['External URL']
            
            # Skip rows with null/empty values for required fields
            if pd.isna(full_path) or pd.isna(external_url) or not full_path or not external_url:
                logger.debug(f"Skipping bucket with null values: full_path={full_path}, external_url={external_url}")
                continue
                
            # Split path into fsp_name and relative path
            path_parts = full_path.split('/', 2)
            if len(path_parts) >= 2:
                fsp_name = path_parts[1]  # First component after leading slash
                relative_path = path_parts[2] if len(path_parts) > 2 else ''
                
                bucket = ExternalBucketDB(
                    full_path=full_path,
                    external_url=external_url,
                    fsp_name=fsp_name,
                    relative_path=relative_path
                )
                all_buckets.append(bucket)
    
    buckets = all_buckets

    # Count how many tables we actually processed
    processed_tables = sum(1 for table in tables if 'External URL' in table.columns and 'Filesystem Path' in table.columns)
    
    logger.debug(f"Processed {processed_tables} tables and found {len(buckets)} external buckets in the wiki")
    return buckets, table_last_updated


def convert_table_to_external_buckets(table):
    """Convert the wiki table to a list of DB objects"""
    return [ExternalBucketDB(
        full_path=row.full_path,
        external_url=row.external_url,
        fsp_name=row.fsp_name,
        relative_path=row.relative_path
    ) for row in table.itertuples(index=False)]
