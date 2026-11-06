"""Міжпроцесне блокування тестової БД (T0.8): другий прогін pytest не зносить схему першому."""

from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy.engine import URL

from tests.db.conftest import db_lock_path, hold_test_db_lock

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(sys.platform == "win32", reason="fcntl — лише POSIX (macOS, WSL2)"),
]


def test_a_second_run_cannot_take_the_test_db_while_this_session_holds_it(migrated_db: URL) -> None:
    import fcntl

    assert migrated_db.database is not None
    with db_lock_path(migrated_db.database).open("a", encoding="utf-8") as other, pytest.raises(BlockingIOError):
        fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_the_same_process_takes_the_lock_again_without_waiting(migrated_db: URL) -> None:
    """Фікстуру реекспортують кілька conftest: повторне взяття в тому самому процесі не чекає на себе."""
    assert migrated_db.database is not None
    # Сесія вже тримає блокування (`migrated_db`): повторне взяття повертається одразу, а не чекає на себе.
    first = hold_test_db_lock(migrated_db.database)
    second = hold_test_db_lock(migrated_db.database)
    assert first is second or first is None or second is None
    assert db_lock_path(migrated_db.database).read_text(encoding="utf-8").strip() == str(os.getpid())
