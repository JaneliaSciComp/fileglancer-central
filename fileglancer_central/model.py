from datetime import datetime
from typing import List, Optional, Dict

from pydantic import BaseModel, Field, HttpUrl


class FileSharePath(BaseModel):
    """A file share path from the database"""
    name: str = Field(
        description="The name of the file share, which uniquely identifies the file share."
    )
    zone: str = Field(
        description="The zone of the file share, for grouping paths in the UI."
    )
    group: Optional[str] = Field(
        description="The group that owns the file share",
        default=None
    )
    storage: Optional[str] = Field(
        description="The storage type of the file share (home, primary, scratch, etc.)",
        default=None
    )
    mount_path: str = Field(
        description="The path where the file share is mounted on the local machine"
    )
    mac_path: Optional[str] = Field(
        description="The path used to mount the file share on Mac (e.g. smb://server/share)",
        default=None
    )
    windows_path: Optional[str] = Field(
        description="The path used to mount the file share on Windows (e.g. \\\\server\\share)",
        default=None
    )
    linux_path: Optional[str] = Field(
        description="The path used to mount the file share on Linux (e.g. /unix/style/path)",
        default=None
    )

class FileSharePathResponse(BaseModel):
    paths: List[FileSharePath] = Field(
        description="A list of file share paths"
    )
    
class TicketComment(BaseModel):
    """A comment on a ticket"""
    author_name: str = Field(
        description="The author of the comment"
    )
    author_display_name: str = Field(
        description="The display name of the author"
    )
    body: str = Field(
        description="The body of the comment"
    )
    
class TicketComment(BaseModel):
    """A comment on a ticket"""
    author_name: str = Field(
        description="The author of the comment"
    )
    author_display_name: str = Field(
        description="The display name of the author"
    )
    body: str = Field(
        description="The body of the comment"
    )
    created: datetime = Field(
        description="The date and time the comment was created"
    )
    updated: datetime = Field(
        description="The date and time the comment was updated"
    )

class Ticket(BaseModel):
    """A JIRA ticket"""
    username: str = Field(
        description="The username of the user who created the ticket"
    )
    path: str = Field(
        description="The path of the file the ticket was created for, relative to the file share path mount point"
    )
    fsp_name: str = Field(
        description="The name of the file share path associated with the file this ticket was created for"
    )
    key: str = Field(
        description="The key of the ticket"
    )
    created: datetime = Field(
        description="The date and time the ticket was created"
    )
    updated: datetime = Field(
        description="The date and time the ticket was updated"
    )
    status: str = Field(
        description="The status of the ticket"
    )
    resolution: str = Field(
        description="The resolution of the ticket"
    )
    description: str = Field(
        description="The description of the ticket"
    )
    link: HttpUrl = Field(
        description="The link to the ticket"
    )
    comments: List[TicketComment] = Field(
        description="The comments on the ticket"
    )
    

class UserPreference(BaseModel):
    """A user preference"""
    key: str = Field(
        description="The key of the preference"
    )
    value: Dict = Field(
        description="The value of the preference"
    )


class ProxiedPath(BaseModel):
    """A proxied path which is used to share a file system path via a URL"""
    username: str = Field(
        description="The username of the user who owns this proxied path"
    )
    # TODO: does this need to be exposed in the API? It's already included in the URL.
    sharing_key: str = Field(
        description="The sharing key is part of the URL proxy path. It is used to uniquely identify the proxied path."
    )
    sharing_name: str = Field(
        description="The sharing path is part of the URL proxy path. It is mainly used to provide file extension information to the client."
    )
    path: str = Field(
        description="The path relative to the file share path mount point"
    )
    fsp_name: str = Field(
        description="The name of the file share path that this proxied path is associated with"
    )
    created_at: datetime = Field(
        description="When this proxied path was created"
    )
    updated_at: datetime = Field(
        description="When this proxied path was last updated"
    )
    url: HttpUrl = Field(
        description="The URL for accessing the data via the proxy"
    )

class ProxiedPathResponse(BaseModel):
    paths: List[ProxiedPath] = Field(
        description="A list of proxied paths"
    )
