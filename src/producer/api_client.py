from __future__ import annotations

from datetime import datetime

import httpx


class HubeauPageLimitError(RuntimeError):
    """Raised when cursor pagination exceeds the configured page cap."""


class HubeauResponseError(ValueError):
    """Raised when a Hub'Eau page is not the expected JSON object."""


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
        max_pages: int,
    ) -> list[dict]:
        """Return every page since date_debut_obs, or raise before a partial result is used."""
        params: dict | None = {
            "code_entite": station_code,
            "grandeur_hydro": grandeur_hydro,
            "date_debut_obs": _format_api_time(date_debut_obs),
            "size": page_size,
        }
        url: str | None = "/observations_tr"
        rows: list[dict] = []
        pages = 0

        while url:
            if pages >= max_pages:
                raise HubeauPageLimitError(
                    f"Hub'Eau returned more than {max_pages} page(s); refusing a partial window"
                )
            response = self._client.get(url, params=params)
            response.raise_for_status()
            page_rows, url = _page(response)
            rows.extend(page_rows)
            params = None
            pages += 1

        return rows


def _format_api_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _page(response) -> tuple[list, str | None]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise HubeauResponseError("Hub'Eau response is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise HubeauResponseError("Hub'Eau response must be a JSON object")
    if "data" not in payload:
        raise HubeauResponseError("Hub'Eau response is missing data")
    data = payload["data"]
    if not isinstance(data, list):
        raise HubeauResponseError("Hub'Eau data must be a list")
    next_url = payload.get("next")
    if next_url is not None and not isinstance(next_url, str):
        raise HubeauResponseError("Hub'Eau next link must be a string or null")
    return data, next_url
