"""
Database connection management for FastAPI.
Uses psycopg2 connection pool for efficient request handling.
"""

import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from app.config import settings

# Connection pool — min 2, max 10 connections
connection_pool = None


def init_pool():
    """Initialize the connection pool. Call on app startup."""
    global connection_pool
    connection_pool = pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=10,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def close_pool():
    """Close the connection pool. Call on app shutdown."""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()


@contextmanager
def get_db():
    """Get a database connection from the pool."""
    conn = connection_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        connection_pool.putconn(conn)