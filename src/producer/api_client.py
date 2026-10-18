from __future__ import annotations

from datetime import datetime

import httpx


class HubeauClient:
    """Reads real-time hydrometric observations from Hub'Eau."""

    def __init__(self, base_url: str, timeout_seconds: float = 30.0) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def fetch_observations(
        self,
        station_code: str,
        grandeur_hydro: str,
        date_debut_obs: datetime,
        page_size: int,
    ) -> list[dict]:
        """Return observations for one station since date_debut_obs, oldest last in the API order."""
        params: dict | None = {
            "code_entite": station_code,
            "grandeur_hydro": grandeur_hydro,
            "date_debut_obs": _format_api_time(date_debut_obs),
            "size": page_size,
        }
        url: str | None = "/observations_tr"
        rows: list[dict] = []

        while url:
            response = self._client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
            rows.extend(payload.get("data") or [])
            url = payload.get("next")
            params = None

        return rows


def _format_api_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")
