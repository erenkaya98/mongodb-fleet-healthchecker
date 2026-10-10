# scheduler.py leader lock test suite
# ===================================
# Guards the three bugs the original implementation had:
#   1. the acquire filter had no expiry condition, so every instance
#      unconditionally stole the lock from a live holder
#   2. expires_at was written as a float, which MongoDB's TTL index ignores
#   3. a run slower than scheduler_lock_ttl silently lost its lock
#
# The metadata DB is mocked; these tests assert on the exact query sent to
# MongoDB, because that query *is* the mutual-exclusion guarantee.

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pymongo.errors import DuplicateKeyError

from app.scheduler import INSTANCE_ID, SchedulerService

# Captured before any patching: patching app.scheduler.asyncio.sleep patches the
# asyncio module itself, so the tests need an unpatched handle to yield control.
_real_sleep = asyncio.sleep


async def _instant_sleep(_seconds):
    """Stand-in for asyncio.sleep that still yields to the event loop."""
    await _real_sleep(0)


def _mock_db(update_result_matched: int = 1, update_side_effect=None):
    """A fake metadata DB whose locks.update_one is observable."""
    db = MagicMock()
    result = MagicMock()
    result.matched_count = update_result_matched
    result.modified_count = update_result_matched
    db.locks.update_one = AsyncMock(return_value=result, side_effect=update_side_effect)
    db.locks.delete_one = AsyncMock()
    return db


@pytest.fixture
def scheduler():
    return SchedulerService()


class TestAcquireLock:
    """_acquire_lock: atomic, single round trip, never displaces a live holder."""

    async def test_acquires_when_free(self, scheduler):
        db = _mock_db()
        with patch("app.scheduler.get_db", return_value=db):
            assert await scheduler._acquire_lock() is True
        db.locks.update_one.assert_awaited_once()

    async def test_duplicate_key_means_held_by_another(self, scheduler):
        """The unique index on locks.name rejecting the insert == lock is taken."""
        db = _mock_db(update_side_effect=DuplicateKeyError("E11000 duplicate key"))
        with patch("app.scheduler.get_db", return_value=db):
            assert await scheduler._acquire_lock() is False

    async def test_unexpected_error_does_not_acquire(self, scheduler):
        db = _mock_db(update_side_effect=RuntimeError("network down"))
        with patch("app.scheduler.get_db", return_value=db):
            assert await scheduler._acquire_lock() is False

    async def test_filter_guards_on_expiry_or_ownership(self, scheduler):
        """Regression: the filter must never match a live holder.

        The original implementation filtered on {"name": ...} alone, which let
        every instance overwrite a lock that another instance was actively
        holding. The $or guard is the whole point of this function.
        """
        db = _mock_db()
        with patch("app.scheduler.get_db", return_value=db):
            await scheduler._acquire_lock()

        query = db.locks.update_one.await_args.args[0]
        assert query["name"] == "scheduler_leader"
        assert "$or" in query, "acquire filter must be conditional, not unconditional"

        branches = query["$or"]
        assert {"instance_id": INSTANCE_ID} in branches, "must be re-entrant for its own holder"

        expiry = next(b for b in branches if "expires_at" in b)
        assert "$lte" in expiry["expires_at"], "must only take over an expired lock"

    async def test_expires_at_is_a_date_not_a_float(self, scheduler):
        """Regression: MongoDB TTL indexes silently ignore non-Date fields.

        Storing a float meant a crashed instance's lock was never reaped.
        """
        db = _mock_db()
        with patch("app.scheduler.get_db", return_value=db):
            await scheduler._acquire_lock()

        update = db.locks.update_one.await_args.args[1]
        expires_at = update["$set"]["expires_at"]
        assert isinstance(expires_at, datetime), (
            f"expires_at must be a BSON Date, got {type(expires_at)}"
        )
        assert expires_at.tzinfo is not None, "expires_at must be timezone-aware"

        query = db.locks.update_one.await_args.args[0]
        cutoff = next(b for b in query["$or"] if "expires_at" in b)["expires_at"]["$lte"]
        assert isinstance(cutoff, datetime), "the expiry comparison must also use a Date"

    async def test_upsert_is_enabled(self, scheduler):
        """Without upsert the very first acquisition on a fresh database fails."""
        db = _mock_db()
        with patch("app.scheduler.get_db", return_value=db):
            await scheduler._acquire_lock()
        assert db.locks.update_one.await_args.kwargs["upsert"] is True


class TestRenewLock:
    async def test_renews_when_still_ours(self, scheduler):
        db = _mock_db(update_result_matched=1)
        with patch("app.scheduler.get_db", return_value=db):
            assert await scheduler._renew_lock() is True

        query = db.locks.update_one.await_args.args[0]
        assert query == {"name": "scheduler_leader", "instance_id": INSTANCE_ID}

    async def test_returns_false_when_lock_was_taken(self, scheduler):
        """matched_count == 0 means another instance owns the document now."""
        db = _mock_db(update_result_matched=0)
        with patch("app.scheduler.get_db", return_value=db):
            assert await scheduler._renew_lock() is False

    async def test_error_is_not_a_successful_renewal(self, scheduler):
        db = _mock_db(update_side_effect=RuntimeError("connection reset"))
        with patch("app.scheduler.get_db", return_value=db):
            assert await scheduler._renew_lock() is False


class TestLockHeartbeat:
    async def test_heartbeat_renews_repeatedly(self, scheduler):
        """Regression: a run slower than the TTL must keep its lock."""
        renew = AsyncMock(return_value=True)
        with (
            patch.object(scheduler, "_renew_lock", renew),
            patch("app.scheduler.asyncio.sleep", new=_instant_sleep),
        ):
            task = asyncio.ensure_future(scheduler._lock_heartbeat())
            for _ in range(50):
                if renew.await_count >= 3:
                    break
                await _real_sleep(0)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert renew.await_count >= 3, "heartbeat must keep renewing while the run is in flight"

    async def test_heartbeat_stops_when_lock_is_lost(self, scheduler):
        """Losing the lock must end the heartbeat rather than renew forever."""
        renew = AsyncMock(return_value=False)
        with (
            patch.object(scheduler, "_renew_lock", renew),
            patch("app.scheduler.asyncio.sleep", new=_instant_sleep),
        ):
            await asyncio.wait_for(scheduler._lock_heartbeat(), timeout=2)

        assert renew.await_count == 1, "must stop at the first failed renewal"


class TestReleaseLock:
    async def test_release_only_deletes_our_own_lock(self, scheduler):
        """Deleting by name alone would drop a lock another instance holds."""
        db = _mock_db()
        with patch("app.scheduler.get_db", return_value=db):
            await scheduler._release_lock()

        query = db.locks.delete_one.await_args.args[0]
        assert query["name"] == "scheduler_leader"
        assert query["instance_id"] == INSTANCE_ID
