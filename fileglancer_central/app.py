import sys
from datetime import datetime
from typing import List, Optional, Dict

from loguru import logger
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.exceptions import RequestValidationError, StarletteHTTPException
from pydantic import BaseModel, Field, HttpUrl

from fileglancer_central.settings import get_settings
from fileglancer_central.database import get_db_session, get_all_paths, get_last_refresh, update_file_share_paths, get_user_preference, set_user_preference, delete_user_preference, get_all_user_preferences
from fileglancer_central.wiki import get_wiki_table
from fileglancer_central.issues import create_jira_ticket, get_jira_ticket_details, delete_jira_ticket


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

def create_app(settings):

    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET","HEAD"],
        allow_headers=["*"],
        expose_headers=["Range", "Content-Range"],
    )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request, exc):
        return JSONResponse({"error":str(exc.detail)}, status_code=exc.status_code)


    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request, exc):
        return JSONResponse({"error":str(exc)}, status_code=400)
    

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Setup
        if callable(settings):
            app.settings = settings()
        else:
            app.settings = settings

        # Configure logging based on the log level in the settings
        logger.remove()
        logger.add(sys.stderr, level=app.settings.log_level)

        logger.info(f"Server ready")
        yield
        # Cleanup (if needed)

    app = FastAPI(lifespan=lifespan)

    @app.get("/", include_in_schema=False)
    async def docs_redirect():
        return RedirectResponse("/docs")


    @app.get("/file-share-paths", response_model=FileSharePathResponse, 
             description="Get all file share paths from the database")
    async def get_file_share_paths(force_refresh: bool = False) -> List[FileSharePath]:
        with get_db_session() as session:
            last_refresh = get_last_refresh(session)
            if not last_refresh or (datetime.now() - last_refresh.db_last_updated).days >= 1 or force_refresh:
                logger.info("Last refresh was more than a day ago, checking for updates...")
                confluence_url = app.settings.confluence_url
                confluence_token = app.settings.confluence_token
                if not confluence_url or not confluence_token:
                    logger.error("You must configure `confluence_url` and `confluence_token` in the config.yaml file")
                    raise HTTPException(status_code=500, detail="Confluence is not configured")
                
                table, table_last_updated = get_wiki_table(confluence_url, confluence_token)

                if not last_refresh or table_last_updated != last_refresh.source_last_updated:
                    logger.info("Wiki table has changed, refreshing file share paths...")
                    update_file_share_paths(session, table, table_last_updated)

            paths = [FileSharePath(
                        name=path.name,
                        zone=path.zone,
                        group=path.group,
                        storage=path.storage,
                        mount_path=path.mount_path,
                        mac_path=path.mac_path, 
                        windows_path=path.windows_path,
                        linux_path=path.linux_path,
                    ) for path in get_all_paths(session)]

        return FileSharePathResponse(paths=paths)



    @app.post("/ticket", response_model=str,
              description="Create a new ticket and return the key")
    async def create_ticket(
        project_key: str,
        issue_type: str,
        summary: str,
        description: str
    ) -> str:
        ticket = create_jira_ticket(
            project_key=project_key,
            issue_type=issue_type, 
            summary=summary,
            description=description
        )
        return ticket['key']
    

    @app.get("/ticket/{ticket_key}", response_model=Ticket, 
             description="Retrieve a ticket by its key")
    async def get_ticket(ticket_key: str):
        try:
            return get_jira_ticket_details(ticket_key)
        except Exception as e:
            if str(e) == "Issue Does Not Exist":
                raise HTTPException(status_code=404, detail=str(e))
            else:
                raise HTTPException(status_code=500, detail=str(e))


    @app.delete("/ticket/{ticket_key}",
                description="Delete a ticket by its key")
    async def delete_ticket(ticket_key: str):
        try:
            delete_jira_ticket(ticket_key)
            return {"message": f"Ticket {ticket_key} deleted"}
        except Exception as e:
            if str(e) == "Issue Does Not Exist":
                raise HTTPException(status_code=404, detail=str(e))
            else:
                raise HTTPException(status_code=500, detail=str(e))


    @app.get("/preference/{username}", response_model=Dict[str, Dict],
             description="Get all preferences for a user")
    async def get_preferences(username: str):
        with get_db_session() as session:
            return get_all_user_preferences(session, username)


    @app.get("/preference/{username}/{key}", response_model=Optional[Dict],
             description="Get a specific preference for a user")
    async def get_preference(username: str, key: str):
        with get_db_session() as session:
            pref = get_user_preference(session, username, key)
            if pref is None:
                raise HTTPException(status_code=404, detail="Preference not found")
            return pref


    @app.put("/preference/{username}/{key}",
             description="Set a preference for a user")
    async def set_preference(username: str, key: str, value: Dict):
        with get_db_session() as session:
            set_user_preference(session, username, key, value)
            return {"message": f"Preference {key} set for user {username}"}


    @app.delete("/preference/{username}/{key}",
                description="Delete a preference for a user")
    async def delete_preference(username: str, key: str):
        with get_db_session() as session:
            deleted = delete_user_preference(session, username, key)
            if not deleted:
                raise HTTPException(status_code=404, detail="Preference not found")
            return {"message": f"Preference {key} deleted for user {username}"}


    return app


app = create_app(get_settings)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, lifespan="on")
