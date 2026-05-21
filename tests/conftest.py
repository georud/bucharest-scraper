import pytest

from src.storage.database import Database


@pytest.fixture
def db(tmp_path):
    """A fresh Database backed by a temp file (schema + migrations applied)."""
    database = Database(db_path=tmp_path / "test.db")
    yield database
    database.close()
