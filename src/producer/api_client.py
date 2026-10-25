from __future__ import annotations

import logging
import time
from datetime import datetime

import httpx

from src.common.log_events import log_event

logger = logging.getLogger(__name__)


class HubeauPageLimitError(RuntimeError):
    """Raised when cursor pagination exceeds the configured page cap."""


class HubeauResponseError(ValueError):
    """Raised when a Hub'Eau page is not the expected JSON object."""


class HubeauClient:
    """Reads real-time hydrometric observations from Hub'Eau."""

    def __init__(self, base_url: str, timeout_seconds: float = 30.0) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout_seconds)
        self.pages_fetched = 0

    def close(self) -> None:
        self._client.close()

    def fetch_observations(
        self,
        station_code: str,
        grandeur_hydro: str,
        date_debut_obs: datetime,
        page_size: int,
        max_pages: int,
        attempt: int = 1,
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
        self.pages_fetched = 0

        while url:
            if pages >= max_pages:
                raise HubeauPageLimitError(
                    f"Hub'Eau returned more than {max_pages} page(s); refusing a partial window"
                )
            page_number = pages + 1
            started = time.perf_counter()
            try:
                response = self._client.get(url, params=params)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                _log_request(
                    "api_request_failed",
                    logging.WARNING,
                    url=url,
                    page=page_number,
                    attempt=attempt,
                    started=started,
                    status=exc.response.status_code,
                    error=exc,
                )
                raise
            except httpx.RequestError as exc:
                _log_request(
                    "api_request_failed",
                    logging.WARNING,
                    url=url,
                    page=page_number,
                    attempt=attempt,
                    started=started,
                    error=exc,
                )
                raise
            _log_request(
                "api_request",
                logging.INFO,
                url=url,
                page=page_number,
                attempt=attempt,
                started=started,
                status=response.status_code,
            )
            page_rows, url = _page(response)
            rows.extend(page_rows)
            params = None
            pages += 1
            self.pages_fetched = pages

        return rows


def _format_api_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _log_request(event: str, level: int, *, url: str, page: int, attempt: int, started: float, status: int | None = None, error: Exception | None = None) -> None:
    log_event(
        logger,
        level,
        event,
        path=url.split("?", 1)[0],
        page=page,
        attempt=attempt,
        duration_ms=int((time.perf_counter() - started) * 1000),
        status=status,
        error=error,
    )


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
