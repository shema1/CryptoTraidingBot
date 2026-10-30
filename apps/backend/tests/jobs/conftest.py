"""Фікстури тестової БД для воркера — ті самі, що в `tests/db` (лише `*_test`)."""

from tests.db.conftest import conn, migrated_db, test_db_url

__all__ = ["conn", "migrated_db", "test_db_url"]
