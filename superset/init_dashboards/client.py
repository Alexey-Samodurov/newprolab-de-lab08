"""Authenticated HTTP client for the Superset REST API."""

from __future__ import annotations

import json
import time
from typing import Any

import requests

from ._log import log


class SupersetClient:
    """Thin Superset REST wrapper with login, CSRF and 429 backoff."""

    def __init__(self, host: str, user: str, password: str) -> None:
        """Initialize and authenticate against the Superset API.

        Args:
            host: Base URL of the Superset instance (e.g.
                ``http://localhost:8088``).
            user: Username for authentication.
            password: Password for authentication.
        """
        self.host = host.rstrip("/")
        self.session = requests.Session()
        self._login(user, password)

    def _login(self, user: str, password: str) -> None:
        """Authenticate and store Bearer + CSRF tokens on the session.

        Args:
            user: Username for the Superset login endpoint.
            password: Password for the Superset login endpoint.

        Raises:
            requests.HTTPError: If login or CSRF token request fails.
        """
        resp = self.session.post(
            f"{self.host}/api/v1/security/login",
            json={"username": user, "password": password, "provider": "db", "refresh": True},
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        csrf = self.session.get(f"{self.host}/api/v1/security/csrf_token/")
        csrf.raise_for_status()
        self.session.headers.update({"X-CSRFToken": csrf.json()["result"]})

    def _check(self, r: requests.Response) -> requests.Response:
        """Raise for non-OK responses after logging the body excerpt.

        Args:
            r: HTTP response to check.

        Returns:
            The same response if it was OK.

        Raises:
            requests.HTTPError: If the response status indicates failure.
        """
        if not r.ok:
            log.error("%s %s %s: %s", r.status_code, r.request.method, r.url, r.text[:400])
            r.raise_for_status()
        return r

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """Issue a request with 429-aware exponential backoff.

        Retries up to 6 times honouring the ``Retry-After`` header when
        present.

        Args:
            method: HTTP method (e.g. ``"GET"``, ``"POST"``).
            path: API path relative to ``self.host``.
            **kwargs: Additional keyword arguments forwarded to
                ``requests.Session.request``.

        Returns:
            The response from the last attempt.
        """
        url = f"{self.host}{path}"
        backoff = 1.0
        for attempt in range(6):
            r = self.session.request(method, url, **kwargs)
            if r.status_code != 429:
                return r
            wait = float(r.headers.get("Retry-After", backoff))
            log.warning("429 rate-limited on %s %s; sleep %.1fs (attempt %d/6)",
                        method, path, wait, attempt + 1)
            time.sleep(wait)
            backoff = min(backoff * 2, 16.0)
        return r

    def get(self, path: str, **kwargs: Any) -> dict:
        """Perform a GET request and return the parsed JSON response.

        Args:
            path: API path relative to the host.
            **kwargs: Additional keyword arguments forwarded to the request.

        Returns:
            Parsed JSON response body.
        """
        return self._check(self._request("GET", path, **kwargs)).json()

    def post(self, path: str, payload: dict) -> dict:
        """Perform a POST request with a JSON payload.

        Args:
            path: API path relative to the host.
            payload: Dictionary to serialise as JSON request body.

        Returns:
            Parsed JSON response body.
        """
        return self._check(self._request("POST", path, json=payload)).json()

    def put(self, path: str, payload: dict) -> dict:
        """Perform a PUT request with a JSON payload.

        Args:
            path: API path relative to the host.
            payload: Dictionary to serialise as JSON request body.

        Returns:
            Parsed JSON response body.
        """
        return self._check(self._request("PUT", path, json=payload)).json()

    def delete(self, path: str) -> None:
        """Perform a DELETE request and raise on failure.

        Args:
            path: API path relative to the host.
        """
        self._check(self._request("DELETE", path))

    def find_id(self, path: str, name_field: str, name: str) -> int | None:
        """Look up a Superset resource by name and return its id.

        Args:
            path: API list endpoint path (e.g. ``"/api/v1/chart/"``).
            name_field: Filter column name (e.g. ``"slice_name"``).
            name: Value to match against ``name_field``.

        Returns:
            Resource id if found, ``None`` otherwise.
        """
        r = self.session.get(
            f"{self.host}{path}",
            params={"q": json.dumps({"filters": [{"col": name_field, "opr": "eq", "value": name}]})},
        )
        r.raise_for_status()
        results = r.json().get("result", [])
        return results[0]["id"] if results else None
