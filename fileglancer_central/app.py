import sys
from datetime import datetime
from typing import List, Optional, Dict

from loguru import logger
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, Response,JSONResponse, PlainTextResponse
from fastapi.exceptions import RequestValidationError, StarletteHTTPException

from fileglancer_central.model import FileSharePath, FileSharePathResponse, Ticket
from fileglancer_central.settings import get_settings
from fileglancer_central.database import *
from fileglancer_central.wiki import get_wiki_table, convert_table_to_file_share_paths
from fileglancer_central.issues import create_jira_ticket, get_jira_ticket_details, delete_jira_ticket
from fileglancer_central.utils import slugify_path

from x2s3.utils import get_read_access_acl, get_nosuchbucket_response, get_error_response
from x2s3.client_file import FileProxyClient


def cache_wiki_paths(confluence_url, confluence_token, force_refresh=False):
    with get_db_session() as session:
        # Get the last refresh time from the database
        last_refresh = get_last_refresh(session)

        # Check if we need to refresh the file share paths
        if not last_refresh or (datetime.now() - last_refresh.db_last_updated).days >= 1 or force_refresh:
            logger.info("Last refresh was more than a day ago, checking for updates...")
            
            # Get updated paths from the wiki
            table, table_last_updated = get_wiki_table(confluence_url, confluence_token)

            new_paths = convert_table_to_file_share_paths(table)

            if not last_refresh or table_last_updated != last_refresh.source_last_updated:
                logger.info("Wiki table has changed, refreshing file share paths...")
                update_file_share_paths(session, new_paths, table_last_updated)

        return [FileSharePath(
            name=path.name,
            zone=path.zone,
            group=path.group,
            storage=path.storage,
            mount_path=path.mount_path,
            mac_path=path.mac_path, 
            windows_path=path.windows_path,
            linux_path=path.linux_path,
        ) for path in get_all_paths(session)]
        


def get_file_proxy_client(sharing_key: str, sharing_name: str) -> FileProxyClient | Response:
    with get_db_session() as session:
        proxied_path = get_proxied_path_by_sharing_key(session, sharing_key)
        if not proxied_path:
            return get_nosuchbucket_response(sharing_name)
        if proxied_path.sharing_name != sharing_name:
            return get_error_response(400, "InvalidArgument", f"Sharing name mismatch for sharing key {sharing_key}", sharing_name)
        return FileProxyClient(proxy_kwargs={
            'target_name': sharing_name,
            'path': proxied_path.mount_path,
        })


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


    @app.get('/robots.txt', response_class=PlainTextResponse)
    def robots():
        return """User-agent: *\nDisallow: /"""


    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request, exc):
        return JSONResponse({"error":str(exc.detail)}, status_code=exc.status_code)


    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request, exc):
        return JSONResponse({"error":str(exc)}, status_code=400)
    

    @asynccontextmanager
    async def lifespan(app: FastAPI):

        # Configure logging based on the log level in the settings
        logger.remove()
        logger.add(sys.stderr, level=settings.log_level)

        logger.info(f"Settings:")
        logger.info(f"  log_level: {settings.log_level}")
        logger.info(f"  db_url: {settings.db_url}")
        logger.info(f"  confluence_url: {settings.confluence_url}")
        logger.info(f"  jira_url: {settings.jira_url}")
        
        logger.info(f"Server ready")
        yield
        # Cleanup (if needed)
        pass

    app = FastAPI(lifespan=lifespan)

    @app.get("/", include_in_schema=False)
    async def docs_redirect():
        return RedirectResponse("/docs")


    @app.get("/file-share-paths", response_model=FileSharePathResponse, 
             description="Get all file share paths from the database")
    async def get_file_share_paths(force_refresh: bool = False) -> List[FileSharePath]:
        
        confluence_url = settings.confluence_url
        confluence_token = settings.confluence_token
        file_share_mounts = settings.file_share_mounts
        if not confluence_url and not confluence_token and not file_share_mounts:
            logger.error("You must configure `confluence_url` and `confluence_token` or set `file_share_mounts`.")
            raise HTTPException(status_code=500, detail="Confluence is not configured")
        
        if confluence_url and confluence_token:
            paths = cache_wiki_paths(confluence_url, confluence_token, force_refresh)
        else:
            paths = [FileSharePath(
                name=slugify_path(path),
                zone='Local',
                group='local',
                storage='local',
                mount_path=path,
                mac_path=path,
                windows_path=path,
                linux_path=path,
            ) for path in file_share_mounts]

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


    @app.get("/files/{sharing_key}/{sharing_name}/{path:path}")
    async def target_dispatcher(request: Request,
                                sharing_key: str,
                                sharing_name: str,
                                path: str,
                                list_type: int = Query(None, alias="list-type"),
                                continuation_token: Optional[str] = Query(None, alias="continuation-token"),
                                delimiter: Optional[str] = Query(None, alias="delimiter"),
                                encoding_type: Optional[str] = Query(None, alias="encoding-type"),
                                fetch_owner: Optional[bool] = Query(None, alias="fetch-owner"),
                                max_keys: Optional[int] = Query(1000, alias="max-keys"),
                                prefix: Optional[str] = Query(None, alias="prefix"),
                                start_after: Optional[str] = Query(None, alias="start-after")):

        if 'acl' in request.query_params:
            return get_read_access_acl()

        client = get_file_proxy_client(sharing_key, sharing_name)
        if isinstance(client, Response):
            return client
        
        if list_type:
            if list_type == 2:
                return await client.list_objects_v2(continuation_token, delimiter, \
                    encoding_type, fetch_owner, max_keys, prefix, start_after)
            else:
                return get_error_response(400, "InvalidArgument", f"Invalid list type {list_type}", path)
        else:
            range_header = request.headers.get("range")
            return await client.get_object(path, range_header)


    @app.head("/files/{sharing_key}/{sharing_name}/{path:path}")
    async def head_object(sharing_key: str, sharing_name: str, path: str):
        try:
            client = get_file_proxy_client(sharing_key, sharing_name)
            if isinstance(client, Response):
                return client
            return await client.head_object(path)
        except:
            logger.opt(exception=sys.exc_info()).info("Error requesting head")
            return get_error_response(500, "InternalError", "Error requesting HEAD", path)


    return app


app = create_app(get_settings())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, lifespan="on")
