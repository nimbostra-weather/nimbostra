"""Authentification Météo-France : Application ID -> jeton Bearer renouvelé automatiquement."""
import os
import time

import requests
from requests.auth import AuthBase

TOKEN_URL = "https://portail-api.meteofrance.fr/token"


class TokenProvider:
    def __init__(self, application_id=None):
        self.application_id = application_id or os.environ["METEOFRANCE_APPLICATION_ID"]
        self._token = None
        self._expires_at = 0.0

    def get(self):
        if self._token is None or time.time() > self._expires_at - 60:
            r = requests.post(
                TOKEN_URL,
                data={"grant_type": "client_credentials"},
                headers={"Authorization": f"Basic {self.application_id}"},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            self._token = data["access_token"]
            self._expires_at = time.time() + int(data.get("expires_in", 3600))
        return self._token


class BearerAuth(AuthBase):
    """À brancher sur une session : s.auth = BearerAuth(TokenProvider())."""

    def __init__(self, provider):
        self.provider = provider

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.provider.get()}"
        return request
