import json
import os
import subprocess
import time
import unittest
from urllib.parse import urlencode


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
CURL_TIMEOUT_SECONDS = int(os.getenv("CURL_TIMEOUT_SECONDS", "10"))
FUNCTIONAL_WAIT_SECONDS = int(os.getenv("FUNCTIONAL_WAIT_SECONDS", "90"))


class CurlResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.body = body

    def json(self):
        return json.loads(self.body)


def curl(path, params=None):
    query = f"?{urlencode(params)}" if params else ""
    url = f"{API_BASE_URL}{path}{query}"
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--max-time",
        str(CURL_TIMEOUT_SECONDS),
        "--write-out",
        "\n%{http_code}",
        url,
    ]

    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )

    if completed.returncode != 0:
        raise AssertionError(
            f"curl a echoue pour {url}\n"
            f"code={completed.returncode}\n"
            f"stderr={completed.stderr.strip()}"
        )

    body, _, status = completed.stdout.rpartition("\n")
    return CurlResponse(int(status), body)


def wait_for_json(path, predicate, timeout_seconds=FUNCTIONAL_WAIT_SECONDS):
    deadline = time.time() + timeout_seconds
    last_payload = None

    while time.time() < deadline:
        response = curl(path)
        if response.status_code == 200:
            last_payload = response.json()
            if predicate(last_payload):
                return last_payload
        time.sleep(3)

    raise AssertionError(
        f"Condition non atteinte sur {path} apres {timeout_seconds}s. "
        f"Derniere reponse: {last_payload!r}"
    )


class ApiContractCurlTests(unittest.TestCase):

    def test_health_returns_ok(self):
        response = curl("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_resources_contract(self):
        response = curl("/resources")

        self.assertEqual(response.status_code, 200)
        resources = response.json()
        self.assertIsInstance(resources, list)
        self.assertGreaterEqual(len(resources), 4)

        first = resources[0]
        self.assertIn("resource_id", first)
        self.assertIn("type", first)
        self.assertIn("zone_id", first)
        self.assertIn("stock_level_pct", first)

    def test_alerts_contract_with_limit(self):
        response = curl("/alerts", params={"limit": 5})

        self.assertEqual(response.status_code, 200)
        alerts = response.json()
        self.assertIsInstance(alerts, list)
        self.assertLessEqual(len(alerts), 5)

    def test_optimize_contract(self):
        response = curl("/optimize")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("generated_at", payload)
        self.assertIn("recommendations", payload)
        self.assertIsInstance(payload["recommendations"], list)

    def test_history_without_data_returns_404(self):
        response = curl("/zones/zone_inconnue/history", params={"minutes": 1})

        self.assertEqual(response.status_code, 404)
        self.assertIn("detail", response.json())


class FunctionalCurlTests(unittest.TestCase):

    def test_pipeline_produces_zone_metrics(self):
        zones = wait_for_json("/zones", lambda payload: len(payload) >= 4)

        zone_ids = {zone["zone_id"] for zone in zones}
        self.assertSetEqual(
            zone_ids,
            {"main_stage", "electro_stage", "rock_stage", "discovery_stage"},
        )

        for zone in zones:
            self.assertIsInstance(zone["headcount_est"], int)
            self.assertGreaterEqual(zone["headcount_est"], 0)
            self.assertIsInstance(zone["density"], (int, float))
            self.assertGreaterEqual(zone["density"], 0)
            self.assertIn("ts", zone)

    def test_history_available_for_a_live_zone(self):
        zones = wait_for_json("/zones", lambda payload: len(payload) > 0)
        zone_id = zones[0]["zone_id"]

        response = curl(f"/zones/{zone_id}/history", params={"minutes": 30})

        self.assertEqual(response.status_code, 200)
        history = response.json()
        self.assertIsInstance(history, list)
        self.assertGreater(len(history), 0)
        self.assertIn("density", history[-1])
        self.assertIn("avg_speed", history[-1])

    def test_prediction_after_enough_history(self):
        zones = wait_for_json("/zones", lambda payload: len(payload) > 0)
        zone_id = zones[0]["zone_id"]

        history = wait_for_json(
            f"/zones/{zone_id}/history",
            lambda payload: len(payload) >= 5,
            timeout_seconds=max(FUNCTIONAL_WAIT_SECONDS, 120),
        )
        self.assertGreaterEqual(len(history), 5)

        response = curl(f"/predict/{zone_id}", params={"horizon_minutes": 30})

        self.assertEqual(response.status_code, 200)
        prediction = response.json()
        self.assertEqual(prediction["zone_id"], zone_id)
        self.assertEqual(prediction["model"], "linear_regression_v1")
        self.assertGreaterEqual(prediction["trained_on_points"], 5)
        self.assertIn("predicted_density", prediction)


if __name__ == "__main__":
    unittest.main(verbosity=2)
