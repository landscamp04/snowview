"""
SnowView API — FastAPI entry point.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import settings
from app.db.database import init_pool, close_pool
from app.routers import resorts, conditions, export, system


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    init_pool()
    print("SnowView API started — database pool initialized")
    yield
    close_pool()
    print("SnowView API stopped — database pool closed")


app = FastAPI(
    title="SnowView API",
    description="Geospatial snow conditions intelligence for California, Colorado, and Washington",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(resorts.router, prefix="/api/resorts", tags=["Resorts"])
app.include_router(conditions.router, prefix="/api/conditions", tags=["Conditions"])
app.include_router(export.router, prefix="/api/export", tags=["Export"])
app.include_router(system.router, prefix="/api", tags=["System"])


@app.get("/")
async def root():
    return {
        "name": "SnowView API",
        "version": "1.0.0",
        "docs": "/docs",
    }