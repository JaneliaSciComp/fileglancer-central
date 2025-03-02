
import sys
from typing import List, Optional

from loguru import logger
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import RedirectResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from fileglancer_central.settings import get_settings, Target
from fileglancer_central.database import get_db_session, get_all_paths



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

    # @app.exception_handler(StarletteHTTPException)
    # async def http_exception_handler(request, exc):
    #     return JSONResponse({"error":str(exc.detail)}, status_code=exc.status_code)


    # @app.exception_handler(RequestValidationError)
    # async def validation_exception_handler(request, exc):
    #     return JSONResponse({"error":str(exc)}, status_code=400)
    

    @app.on_event("startup")
    async def startup_event():
        """ Runs once when the service is first starting.
            Reads the configuration and sets up the proxy clients. 
        """
        if callable(settings):
            app.settings = settings()
        else:
            app.settings = settings

        # Configure logging
        logger.remove()
        logger.add(sys.stderr, level=app.settings.log_level)

        logger.info(f"Server ready")


    @app.get("/", include_in_schema=False)
    async def docs_redirect():
        return RedirectResponse("/docs")


    class FileSharePath(BaseModel):
        """A file share path from the database"""
        lab: str
        """The lab that owns the file share"""
        storage: str
        """The storage type of the file share"""
        mac_path: str
        """The path to the file share on the Mac"""
        smb_path: str
        """The path to the file share on the Windows network"""
        linux_path: str
        """The path to the file share on the Linux server"""
        ad_group: str

    @app.get("/paths", response_model=List[FileSharePath], 
             description="Get all file share paths from the database")
    async def get_paths() -> List[FileSharePath]:
        session = get_db_session()
        paths = get_all_paths(session)
        
        return [FileSharePath(
            lab=path.lab,
            storage=path.storage,
            mac_path=path.mac_path, 
            smb_path=path.smb_path,
            linux_path=path.linux_path,
            ad_group=path.ad_group
        ) for path in paths]

    return app

app = create_app(get_settings)

if __name__ == "__main__":
    import uvicorn
    print(app)
    uvicorn.run(app, host="0.0.0.0", port=8000, lifespan="on")

