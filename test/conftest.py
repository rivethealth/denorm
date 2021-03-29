import pg
import pytest


@pytest.fixture(scope="session")
def pg_server():
    with pg.open_server():
        yield


@pytest.fixture()
def pg_database(pg_server):
    with pg.connection("dbname=postgres") as conn:
        conn.autocommit = True
        with pg.open_database(conn, "test"):
            yield
