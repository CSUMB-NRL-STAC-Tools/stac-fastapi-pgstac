"""FastAPI application using PGStac.

Enables the extensions specified as a comma-delimited list in
the ENABLED_EXTENSIONS environment variable (e.g. `transactions,sort,query`).
If the variable is not set, enables all extensions.
"""

import os
from contextlib import asynccontextmanager

from brotli_asgi import BrotliMiddleware
from fastapi import FastAPI, APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.middleware import CORSMiddleware, ProxyHeaderMiddleware
from stac_fastapi.api.models import (
    EmptyRequest,
    ItemCollectionUri,
    JSONResponse,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
)
from stac_fastapi.extensions.core import (
    CollectionSearchExtension,
    CollectionSearchFilterExtension,
    FieldsExtension,
    FreeTextExtension,
    ItemCollectionFilterExtension,
    OffsetPaginationExtension,
    SearchFilterExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.core.fields import FieldsConformanceClasses
from stac_fastapi.extensions.core.free_text import FreeTextConformanceClasses
from stac_fastapi.extensions.core.query import QueryConformanceClasses
from stac_fastapi.extensions.core.sort import SortConformanceClasses
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from starlette.middleware import Middleware

from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient, health_check
from stac_fastapi.pgstac.db import close_db_connection, connect_to_db
from stac_fastapi.pgstac.extensions import QueryExtension
from stac_fastapi.pgstac.extensions.filter import FiltersClient
from stac_fastapi.pgstac.transactions import BulkTransactionsClient, TransactionsClient
from stac_fastapi.pgstac.types.search import PgstacSearch

from stac_catalog import parser, gather_reports, db_util

settings = Settings()

# search extensions
search_extensions_map = {
    "query": QueryExtension(),
    "sort": SortExtension(),
    "fields": FieldsExtension(),
    "filter": SearchFilterExtension(client=FiltersClient()),
    "pagination": TokenPaginationExtension(),
}

# collection_search extensions
cs_extensions_map = {
    "query": QueryExtension(conformance_classes=[QueryConformanceClasses.COLLECTIONS]),
    "sort": SortExtension(conformance_classes=[SortConformanceClasses.COLLECTIONS]),
    "fields": FieldsExtension(conformance_classes=[FieldsConformanceClasses.COLLECTIONS]),
    "filter": CollectionSearchFilterExtension(client=FiltersClient()),
    "free_text": FreeTextExtension(
        conformance_classes=[FreeTextConformanceClasses.COLLECTIONS],
    ),
    "pagination": OffsetPaginationExtension(),
}

# item_collection extensions
itm_col_extensions_map = {
    "query": QueryExtension(
        conformance_classes=[QueryConformanceClasses.ITEMS],
    ),
    "sort": SortExtension(
        conformance_classes=[SortConformanceClasses.ITEMS],
    ),
    "fields": FieldsExtension(conformance_classes=[FieldsConformanceClasses.ITEMS]),
    "filter": ItemCollectionFilterExtension(client=FiltersClient()),
    "pagination": TokenPaginationExtension(),
}

enabled_extensions = {
    *search_extensions_map.keys(),
    *cs_extensions_map.keys(),
    *itm_col_extensions_map.keys(),
    "collection_search",
}

if ext := os.environ.get("ENABLED_EXTENSIONS"):
    enabled_extensions = set(ext.split(","))

application_extensions = []

with_transactions = os.environ.get("ENABLE_TRANSACTIONS_EXTENSIONS", "").lower() in [
    "yes",
    "true",
    "1",
]
if with_transactions:
    application_extensions.append(
        TransactionExtension(
            client=TransactionsClient(),
            settings=settings,
            response_class=JSONResponse,
        ),
    )

    application_extensions.append(
        BulkTransactionExtension(client=BulkTransactionsClient()),
    )

# /search models
search_extensions = [
    extension
    for key, extension in search_extensions_map.items()
    if key in enabled_extensions
]
post_request_model = create_post_request_model(search_extensions, base_model=PgstacSearch)
get_request_model = create_get_request_model(search_extensions)
application_extensions.extend(search_extensions)

# /collections/{collectionId}/items model
items_get_request_model = ItemCollectionUri
itm_col_extensions = [
    extension
    for key, extension in itm_col_extensions_map.items()
    if key in enabled_extensions
]
if itm_col_extensions:
    items_get_request_model = create_request_model(
        model_name="ItemCollectionUri",
        base_model=ItemCollectionUri,
        extensions=itm_col_extensions,
        request_type="GET",
    )
    application_extensions.extend(itm_col_extensions)

# /collections model
collections_get_request_model = EmptyRequest
if "collection_search" in enabled_extensions:
    cs_extensions = [
        extension
        for key, extension in cs_extensions_map.items()
        if key in enabled_extensions
    ]
    collection_search_extension = CollectionSearchExtension.from_extensions(cs_extensions)
    collections_get_request_model = collection_search_extension.GET
    application_extensions.append(collection_search_extension)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""
    await connect_to_db(app, add_write_connection_pool=with_transactions)
    yield
    await close_db_connection(app)


api = StacApi(
    app=FastAPI(
        openapi_url=settings.openapi_url,
        docs_url=settings.docs_url,
        redoc_url=None,
        root_path=settings.root_path,
        title=settings.stac_fastapi_title,
        version=settings.stac_fastapi_version,
        description=settings.stac_fastapi_description,
        lifespan=lifespan,
    ),
    settings=settings,
    extensions=application_extensions,
    client=CoreCrudClient(pgstac_search_model=post_request_model),
    response_class=JSONResponse,
    items_get_request_model=items_get_request_model,
    search_get_request_model=get_request_model,
    search_post_request_model=post_request_model,
    collections_get_request_model=collections_get_request_model,
    middlewares=[
        Middleware(BrotliMiddleware),
        Middleware(ProxyHeaderMiddleware),
        Middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=settings.cors_methods,
        ),
    ],
    health_check=health_check,
)
app = api.app

# --- START CUSTOM ENDPOINTS ---
# Create a new FastAPI APIRouter for your custom endpoints
custom_router = APIRouter(
    prefix="/parse",  # e.g., /custom/my_endpoint
    tags=["My Custom Endpoints"], # For documentation in /docs
)

# Define a Pydantic model for your request body (optional, but good practice for POST/PUT)
class URLPostRequest(BaseModel):
    url: str

# Define a simple GET endpoint
@custom_router.post("/file")
async def parse_report(request_body: URLPostRequest):
    url = request_body.url
    filename = parser.get_filename_from_url(url)
    file_content = gather_reports.get_file_content(url)
    dropsonde_report = parser.parse_temp_drop(file_content, filename)
    stac_item = parser.convert_dropsonde_to_stac_item(dropsonde_report, url)
    print(f"STAC Item ID: {stac_item.id}")
    print(f"STAC Item Properties: {stac_item.properties}")
    db_util.add_item_to_catlog(stac_item)
    return {"stac_item_id": stac_item.id, "detail": "STAC item added to the catalog successfully."}

def process_archive(url):
    for item_url in (gather_reports.iter_urls_from_archive_page(url)):
        filename = parser.get_filename_from_url(item_url)
        file_content = gather_reports.get_file_content(item_url)
        dropsonde_report = parser.parse_temp_drop(file_content, filename)
        stac_item = parser.convert_dropsonde_to_stac_item(dropsonde_report, item_url)
        print(f"STAC Item ID: {stac_item.id}")
        print(f"STAC Item Properties: {stac_item.properties}")
        db_util.add_item_to_catlog(stac_item)

@custom_router.post("/archive")
async def parse_report(request_body: URLPostRequest, background_tasks: BackgroundTasks):
    url = request_body.url
    background_tasks.add_task(process_archive, url)
    return {"detail": f"Processing message archive {url}!"}

# Include your custom router in the main FastAPI application
app.include_router(custom_router)

# --- END CUSTOM ENDPOINTS ---

def run():
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        uvicorn.run(
            "stac_fastapi.pgstac.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="info",
            reload=settings.reload,
            root_path=os.getenv("UVICORN_ROOT_PATH", ""),
        )
    except ImportError as e:
        raise RuntimeError("Uvicorn must be installed in order to use command") from e


if __name__ == "__main__":
    run()


def create_handler(app):
    """Create a handler to use with AWS Lambda if mangum available."""
    try:
        from mangum import Mangum

        return Mangum(app)
    except ImportError:
        return None


handler = create_handler(app)
