from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REQUIRED_DATABASE_ENV = ("POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)


def database_settings_from_env(env: Mapping[str, str]) -> DatabaseSettings:
    missing = [name for name in REQUIRED_DATABASE_ENV if not env.get(name)]
    if missing:
        raise RuntimeError("missing database environment variables: " + ", ".join(missing))
    return DatabaseSettings(
        host=env.get("POSTGRES_HOST") or "localhost",
        port=int(env["POSTGRES_PORT"]),
        dbname=env["POSTGRES_DB"],
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
    )


def load_database_settings() -> DatabaseSettings:
    load_dotenv(PROJECT_ROOT / ".env")
    return database_settings_from_env(os.environ)


def connect(settings: DatabaseSettings) -> psycopg.Connection:
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
        autocommit=False,
    )
