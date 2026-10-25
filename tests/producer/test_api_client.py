import json
import unittest
from datetime import datetime, timezone

import httpx

from src.producer.api_client import HubeauClient, HubeauPageLimitError, HubeauResponseError


class PaginationTests(unittest.TestCase):
    def test_follows_the_cursor_until_the_last_page(self) -> None:
        client = HubeauClient("https://hubeau.eaufrance.fr/api/v2/hydrometrie")
        fake = FakeHttp(
            [
                response({"data": [{"date_obs": "a"}], "next": "https://hubeau.eaufrance.fr/next"}),
                response({"data": [{"date_obs": "b"}], "next": None}),
            ]
        )
        client._client = fake

        rows = client.fetch_observations(
            "Y251002001",
            "H",
            datetime(2026, 10, 20, tzinfo=timezone.utc),
            page_size=500,
            max_pages=10,
        )

        self.assertEqual(rows, [{"date_obs": "a"}, {"date_obs": "b"}])
        self.assertEqual(fake.calls[0][0], "/observations_tr")
        self.assertEqual(fake.calls[0][1]["code_entite"], "Y251002001")
        self.assertEqual(fake.calls[1], ("https://hubeau.eaufrance.fr/next", None))

    def test_second_page_failure_does_not_return_a_partial_window(self) -> None:
        client = HubeauClient("https://hubeau.eaufrance.fr/api/v2/hydrometrie")
        client._client = FakeHttp(
            [
                response({"data": [{"date_obs": "a"}], "next": "https://hubeau.eaufrance.fr/next"}),
                response({"data": []}, status=503),
            ]
        )

        with self.assertRaises(httpx.HTTPStatusError):
            client.fetch_observations(
                "Y251002001",
                "H",
                datetime(2026, 10, 20, tzinfo=timezone.utc),
                page_size=500,
                max_pages=10,
            )

    def test_page_cap_stops_before_another_request(self) -> None:
        client = HubeauClient("https://hubeau.eaufrance.fr/api/v2/hydrometrie")
        fake = FakeHttp(
            [
                response({"data": [{"date_obs": "a"}], "next": "https://hubeau.eaufrance.fr/page-2"}),
                response({"data": [{"date_obs": "b"}], "next": "https://hubeau.eaufrance.fr/page-3"}),
                response({"data": [{"date_obs": "c"}], "next": None}),
            ]
        )
        client._client = fake

        with self.assertRaises(HubeauPageLimitError):
            client.fetch_observations(
                "Y251002001",
                "H",
                datetime(2026, 10, 20, tzinfo=timezone.utc),
                page_size=500,
                max_pages=2,
            )

        self.assertEqual(len(fake.calls), 2)

    def test_rejects_a_response_that_is_not_an_object(self) -> None:
        client = _client(response(["row"]))
        with self.assertRaises(HubeauResponseError):
            _fetch(client)

    def test_rejects_data_that_is_not_a_list(self) -> None:
        client = _client(response({"data": {"date_obs": "a"}, "next": None}))
        with self.assertRaises(HubeauResponseError):
            _fetch(client)

    def test_rejects_a_missing_data_field(self) -> None:
        client = _client(response({"next": None}))
        with self.assertRaises(HubeauResponseError):
            _fetch(client)

    def test_rejects_a_next_link_that_is_not_a_string(self) -> None:
        client = _client(response({"data": [], "next": 3}))
        with self.assertRaises(HubeauResponseError):
            _fetch(client)

    def test_rejects_invalid_json(self) -> None:
        client = _client(response(None, error=json.JSONDecodeError("bad", "doc", 0)))
        with self.assertRaises(HubeauResponseError):
            _fetch(client)

    def test_malformed_second_page_does_not_return_the_first_page(self) -> None:
        client = HubeauClient("https://hubeau.eaufrance.fr/api/v2/hydrometrie")
        client._client = FakeHttp(
            [
                response({"data": [{"date_obs": "a"}], "next": "https://hubeau.eaufrance.fr/next"}),
                response(["not-an-object"]),
            ]
        )

        with self.assertRaises(HubeauResponseError):
            _fetch(client)


class FakeResponse:
    def __init__(self, payload, status, error=None):
        self._payload = payload
        self._error = error
        self.status_code = status
        self.request = httpx.Request("GET", "https://hubeau.eaufrance.fr/observations_tr")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "error",
                request=self.request,
                response=httpx.Response(self.status_code, request=self.request),
            )

    def json(self):
        if self._error is not None:
            raise self._error
        return self._payload


def response(payload, status=200, error=None) -> FakeResponse:
    return FakeResponse(payload, status, error)


def _client(page: FakeResponse) -> HubeauClient:
    client = HubeauClient("https://hubeau.eaufrance.fr/api/v2/hydrometrie")
    client._client = FakeHttp([page])
    return client


def _fetch(client: HubeauClient):
    return client.fetch_observations(
        "Y251002001",
        "H",
        datetime(2026, 10, 20, tzinfo=timezone.utc),
        page_size=500,
        max_pages=10,
    )


class FakeHttp:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        return self._responses.pop(0)


if __name__ == "__main__":
    unittest.main()
