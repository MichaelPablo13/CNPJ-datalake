import base64
import json
import os
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from uuid import uuid4


class ObservabilitySmokeTests(unittest.TestCase):
    """Smoke tests for Pushgateway -> Prometheus -> Grafana pipeline."""

    @classmethod
    def setUpClass(cls):
        cls.pushgateway_url = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
        cls.prometheus_url = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
        cls.grafana_url = os.getenv("GRAFANA_URL", "http://localhost:3000")
        cls.grafana_user = os.getenv("GRAFANA_ADMIN_USER", "admin")
        cls.grafana_password = os.getenv("GRAFANA_ADMIN_PASSWORD", "admin")

        cls._probe_service_or_skip(cls.pushgateway_url + "/metrics", "Pushgateway")
        cls._probe_service_or_skip(cls.prometheus_url + "/-/healthy", "Prometheus")
        cls._probe_service_or_skip(cls.grafana_url + "/api/health", "Grafana")

    @classmethod
    def _probe_service_or_skip(cls, url: str, service_name: str) -> None:
        try:
            req = urllib.request.Request(url=url, method="GET")
            with urllib.request.urlopen(req, timeout=8) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"{service_name} respondeu HTTP {resp.status}")
        except Exception as exc:  # noqa: BLE001
            raise unittest.SkipTest(f"{service_name} indisponivel em {url}: {exc}") from exc

    @staticmethod
    def _http_request(
        url: str,
        method: str = "GET",
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 10,
    ) -> tuple[int, str]:
        req = urllib.request.Request(url=url, method=method, data=body)
        for key, value in (headers or {}).items():
            req.add_header(key, value)

        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
            return resp.status, payload

    def _grafana_auth_headers(self) -> dict[str, str]:
        token = base64.b64encode(f"{self.grafana_user}:{self.grafana_password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    def test_01_services_health_endpoints(self):
        status, _ = self._http_request(self.pushgateway_url + "/metrics")
        self.assertEqual(status, 200)

        status, _ = self._http_request(self.prometheus_url + "/-/healthy")
        self.assertEqual(status, 200)

        status, body = self._http_request(self.grafana_url + "/api/health")
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(payload.get("database"), "ok")

    def test_02_pushgateway_to_prometheus_flow(self):
        job_name = f"cnpj_observability_smoke_{uuid4().hex[:10]}"
        metric_name = "cnpj_observability_smoke_runs_total"
        delete_url = f"{self.pushgateway_url}/metrics/job/{job_name}"
        push_url = delete_url

        try:
            self._http_request(delete_url, method="DELETE")
        except urllib.error.HTTPError:
            pass

        metric_line = (
            f'{metric_name}{{stage="smoke",status="success"}} 1\n'
        ).encode("utf-8")
        status, _ = self._http_request(
            push_url,
            method="POST",
            body=metric_line,
            headers={"Content-Type": "text/plain"},
        )
        self.assertEqual(status, 200)

        query = urllib.parse.quote(f'{metric_name}{{job="{job_name}"}}', safe="")
        query_url = f"{self.prometheus_url}/api/v1/query?query={query}"

        result = []
        for _ in range(8):
            _, body = self._http_request(query_url)
            payload = json.loads(body)
            result = payload.get("data", {}).get("result", [])
            if result:
                break
            time.sleep(2)

        self.assertTrue(result, "Prometheus nao retornou metrica do job de smoke")

        labels = result[0].get("metric", {})
        self.assertEqual(labels.get("job"), job_name)
        self.assertEqual(labels.get("__name__"), metric_name)
        self.assertEqual(labels.get("stage"), "smoke")

        self._http_request(delete_url, method="DELETE")

    def test_03_grafana_datasource_and_dashboard(self):
        status, body = self._http_request(
            self.grafana_url + "/api/datasources/name/Prometheus",
            headers=self._grafana_auth_headers(),
        )
        self.assertEqual(status, 200)
        ds = json.loads(body)
        self.assertEqual(ds.get("type"), "prometheus")

        status, body = self._http_request(
            self.grafana_url + "/api/dashboards/uid/cnpj-pipeline-overview",
            headers=self._grafana_auth_headers(),
        )
        self.assertEqual(status, 200)
        dashboard_payload = json.loads(body)
        self.assertEqual(dashboard_payload.get("dashboard", {}).get("uid"), "cnpj-pipeline-overview")


if __name__ == "__main__":
    unittest.main()
