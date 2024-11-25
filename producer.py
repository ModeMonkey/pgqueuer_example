from __future__ import annotations

import asyncio
import sys

import asyncpg

from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

import os

async def conn_db(db_name: str):
    conn = await asyncpg.connect(
        user="test1234",
        password="test1234",
        database=db_name,
        host="localhost",
        port=5332,
    )
    return conn

async def create_database(db_name: str):
    print(f"Checking if {db_name} database exists")
    conn = await conn_db("postgres")
    # Check if db_name exists already
    result = await conn.fetchrow(
        "SELECT 1 FROM pg_catalog.pg_database WHERE datname = $1", db_name
    )
    if not result:
        await conn.execute(f'CREATE DATABASE {db_name}')
        print(f"Database {db_name} created")
    else:
        print(f"Database {db_name} already exists")

async def check_table_exists(conn, table_name: str):
    print(f"Checking if {table_name} table already exists")
    query = """
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = $1
        );
    """
    result = await conn.fetchval(query, table_name)
    print(f"{result=}")
    return result

async def setup_database(db_name: str):
    print(f"Checking if {db_name} should be set up")
    conn = await conn_db(db_name)
    tables = ["pgqueuer", "pgqueuer_schedules", "pgqueuer_statistics"]
    tables_that_exist = 0
    for table in tables:
        result = await check_table_exists(conn, table)
        if result:
            tables_that_exist += 1
    if len(tables) == tables_that_exist:
        print("All tables appear to already be set up")
    elif tables_that_exist:
        raise RuntimeError(f"It appears only {tables_that_exist} of {len(tables)} tables already exist.  Suggest deleting database and its tables to try again.")
    else:
        print("Setting up tables")
        os.system(f"pgq install --pg-host localhost --pg-port 5332 --pg-user test1234 --pg-password test1234 --pg-database {db_name}")

async def main(N: int) -> None:
    await create_database("test_pgqueuer")
    await setup_database("test_pgqueuer")
    print(f"enqueuing {N} items")
    conn = await asyncpg.connect(
        user="test1234",
        password="test1234",
        database="test_pgqueuer",
        host="localhost",
        port=5332,
    )
    driver = AsyncpgDriver(conn)
    queries = Queries(driver)
    await queries.enqueue(
        ["fetch"] * N,
        [f"this is from me: {n}".encode() for n in range(1, N + 1)],
        [0] * N,
    )


if __name__ == "__main__":
    N = 1_000 if len(sys.argv) == 1 else int(sys.argv[1])
    asyncio.run(main(N))
    # asyncio.run(setup_database("test_pgqueuer"))
    # asyncio.run(create_database("test_pgqueuer"))