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
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


if __name__ == "__main__":  

    # SQLAlchemy setup
    Base = declarative_base()
    engine = create_engine('sqlite:///fileglancer.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    class FileSharePathDB(Base):
        __tablename__ = 'file_share_paths'
        
        id = Column(Integer, primary_key=True, autoincrement=True)
        lab = Column(String)
        storage = Column(String)
        mac_path = Column(String)
        smb_path = Column(String)
        linux_path = Column(String, index=True, unique=True)
        ad_group = Column(String, index=True)

    Base.metadata.create_all(engine)

    def parse_iso_timestamp(timestamp):
        return datetime.fromisoformat(timestamp)


    confluence_url = "https://wikis.janelia.org"
    confluence_pat = os.environ.get('CONFLUENCE_TOKEN')
    confluence = Confluence(url=confluence_url, token=confluence_pat)
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


    for index, row in table.iterrows():
        file_share_path_db = FileSharePathDB(
            lab=table.columns[0],
            storage=table.columns[1],
            mac_path=table.columns[2],
            smb_path=table.columns[3],
            linux_path=table.columns[4],
            ad_group=table.columns[5]
        )
        session.add(file_share_path_db)

    session.commit()

    #unique_values_column_1 = table[table.columns[1]].unique()
    #print("Unique values for column 1:", unique_values_column_1)
    #import json
    #print(json.dumps(file_share_paths[0].model_dump(), indent=4))
