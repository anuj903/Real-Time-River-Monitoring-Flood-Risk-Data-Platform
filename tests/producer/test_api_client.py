import unittest
from datetime import datetime, timezone

import httpx

from src.producer.api_client import HubeauClient, HubeauPageLimitError


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


class FakeResponse:
    def __init__(self, payload, status):
        self._payload = payload
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
        return self._payload


def response(payload, status=200) -> FakeResponse:
    return FakeResponse(payload, status)


class FakeHttp:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        return self._responses.pop(0)


if __name__ == "__main__":
    unittest.main()
