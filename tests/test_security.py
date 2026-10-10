# Authentication and credential-exposure test suite
# =================================================
# Guards two things that are easy to regress and expensive to get wrong:
#   1. a target's mongo_uri must never leave the process in full — it carries
#      the password of a monitored production node
#   2. every endpoint except the health probes must require the token when
#      HC_AUTH_TOKEN is set

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

import app.security as security
from app.config import settings
from app.main import app
from app.models import TargetInDB, TargetOut, mask_mongo_uri

PASSWORD = "SuperSecret123"
TARGET = TargetInDB(
    id="507f1f77bcf86cd799439011",
    ip="10.0.0.1",
    hostname="db01",
    mongo_uri=f"mongodb://hc_user:{PASSWORD}@10.0.0.1:27017/?authSource=admin",
)


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def with_token(monkeypatch):
    """Enable authentication with a known token."""
    from pydantic import SecretStr

    monkeypatch.setattr(settings, "auth_token", SecretStr("tok123"))
    return "tok123"


@pytest.fixture
def no_token(monkeypatch):
    monkeypatch.setattr(settings, "auth_token", None)


class TestMaskMongoUri:
    @pytest.mark.parametrize(
        ("uri", "expected"),
        [
            ("mongodb://u:p@h:27017/db", "mongodb://u:***@h:27017/db"),
            ("mongodb+srv://u:p@c.example.net/db", "mongodb+srv://u:***@c.example.net/db"),
            # password containing '@' — the host separator is the *last* '@'
            ("mongodb://u:p@ss@h:27017", "mongodb://u:***@h:27017"),
            (
                "mongodb://u:p@a:27017,b:27017/?replicaSet=rs",
                "mongodb://u:***@a:27017,b:27017/?replicaSet=rs",
            ),
            # nothing to mask
            ("mongodb://h:27017", "mongodb://h:27017"),
            ("mongodb://user@h:27017", "mongodb://user@h:27017"),
            (None, None),
            ("", ""),
        ],
    )
    def test_masking(self, uri, expected):
        assert mask_mongo_uri(uri) == expected

    def test_target_out_never_carries_the_password(self):
        out = TargetOut.from_db(TARGET)
        assert PASSWORD not in out.model_dump_json()
        assert out.has_mongo_uri is True
        assert ":***@" in out.mongo_uri


class TestCredentialExposure:
    """Regression: /api/targets used to return the raw connection string."""

    def test_target_list_does_not_leak_the_password(self, client, no_token):
        with patch("app.api.get_targets", new=AsyncMock(return_value=[TARGET])):
            response = client.get("/api/targets")
        assert response.status_code == 200
        assert PASSWORD not in response.text
        assert ":***@" in response.text

    def test_edit_form_does_not_leak_the_password(self, client, no_token):
        with patch("app.api.get_target", new=AsyncMock(return_value=TARGET)):
            response = client.get(f"/targets/{TARGET.id}/edit-form")
        assert response.status_code == 200
        assert PASSWORD not in response.text

    def test_masked_uri_sent_back_does_not_overwrite_the_stored_one(self, client, no_token):
        """The UI only ever sees the masked value; echoing it back must be a no-op."""
        update = AsyncMock(return_value=TARGET)
        with patch("app.api.update_target", new=update):
            client.put(
                f"/api/targets/{TARGET.id}",
                json={"mongo_uri": "mongodb://hc_user:***@10.0.0.1:27017/?authSource=admin"},
            )
        sent = update.await_args.args[1]
        assert sent.mongo_uri is None, "the masked placeholder must be dropped, not stored"


class TestAuthDisabled:
    def test_everything_is_open_without_a_token(self, client, no_token):
        assert security.auth_enabled() is False
        with patch("app.api.get_targets", new=AsyncMock(return_value=[])):
            assert client.get("/api/targets").status_code == 200


class TestAuthEnabled:
    def test_api_requires_a_token(self, client, with_token):
        assert client.get("/api/targets").status_code == 401

    def test_wrong_token_is_rejected(self, client, with_token):
        response = client.get("/api/targets", headers={"Authorization": "Bearer nope"})
        assert response.status_code == 401

    def test_bearer_token_is_accepted(self, client, with_token):
        with patch("app.api.get_targets", new=AsyncMock(return_value=[])):
            response = client.get("/api/targets", headers={"Authorization": f"Bearer {with_token}"})
        assert response.status_code == 200

    def test_cookie_is_accepted(self, client, with_token):
        client.cookies.set(security.COOKIE_NAME, with_token)
        with patch("app.api.get_targets", new=AsyncMock(return_value=[])):
            response = client.get("/api/targets")
        assert response.status_code == 200
        client.cookies.clear()

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("delete", "/api/targets/1"),
            ("post", "/api/run-now"),
            ("post", "/api/targets/1/toggle"),
            ("post", "/api/alerts/abc/silence"),
            ("get", "/metrics"),
            ("get", "/api/status"),
        ],
    )
    def test_write_and_data_endpoints_are_guarded(self, client, with_token, method, path):
        assert getattr(client, method)(path).status_code == 401

    def test_health_probes_stay_public(self, client, with_token):
        # readyz may answer 503 when the database is unreachable — the point is
        # that it is not a 401.
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code != 401

    def test_browser_navigation_redirects_to_login(self, client, with_token):
        response = client.get("/", headers={"Accept": "text/html"}, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/login"

    def test_login_sets_the_cookie(self, client, with_token):
        response = client.post("/login", data={"token": with_token}, follow_redirects=False)
        assert response.status_code == 303
        assert security.COOKIE_NAME in response.headers.get("set-cookie", "")
        assert "httponly" in response.headers["set-cookie"].lower()

    def test_login_rejects_a_wrong_token(self, client, with_token):
        response = client.post("/login", data={"token": "nope"}, follow_redirects=False)
        assert response.status_code == 401
        assert security.COOKIE_NAME not in response.headers.get("set-cookie", "")

    def test_token_comparison_handles_empty_input(self, with_token):
        assert security.token_is_valid(None) is False
        assert security.token_is_valid("") is False
        assert security.token_is_valid(with_token) is True
