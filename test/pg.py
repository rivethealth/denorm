import contextlib
import os
import subprocess
import time

import psycopg2
import psycopg2.sql as sql


@contextlib.contextmanager
def open_server():
    name = "db-slice-test"
    process = subprocess.Popen(
        [
            "docker",
            "run",
            "--rm",
            "--name",
            name,
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "-p",
            "15432:5432",
            "postgres",
            "-c",
            "fsync=off",
            "-c",
            "full_page_writes=off",
            "-c",
            "max_wal_senders=0",
            "-c",
            "synchronous_commit=off",
            "-c",
            "wal_level=minimal",
        ]
    )
    try:
        while subprocess.run(
            ["docker", "exec", name, "pg_isready", "-h", "localhost"]
        ).returncode:
            time.sleep(0.15)
        yield
    finally:
        process.terminate()
        code = process.wait()
        if code:
            raise Exception(f"Exited with code {code}")


@contextlib.contextmanager
def open_database(conn, name):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))

    yield

    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


@contextlib.contextmanager
def connection(*args, **kwargs):
    conn = psycopg2.connect(*args, **kwargs)
    try:
        yield conn
    finally:
        conn.close()


@contextlib.contextmanager
def transaction(conn):
    with conn:
        with conn.cursor() as cur:
            yield cur
