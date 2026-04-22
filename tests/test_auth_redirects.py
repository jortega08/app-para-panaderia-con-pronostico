from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

from flask import Flask


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("ENABLE_IN_APP_SCHEDULER", "0")

from app.web.auth import _redirect_post_login, auth_bp


class AuthRedirectsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.app.secret_key = "test-secret"
        self.app.register_blueprint(auth_bp)
        self.app.add_url_rule("/cajero/pos", "cajero_pos", lambda: "ok")
        self.app.add_url_rule("/mesero/mesas", "mesero_mesas", lambda: "ok")
        self.app.add_url_rule("/platform", "platform_panel", lambda: "ok")
        self.app.add_url_rule("/panadero/pronostico", "panadero_pronostico", lambda: "ok")

    def test_panadero_with_must_change_password_redirects_to_dashboard(self) -> None:
        with self.app.test_request_context("/login"):
            response = _redirect_post_login({"rol": "panadero", "must_change_password": 1})

        self.assertEqual(response.status_code, 302)
        self.assertIn("/panadero/pronostico", response.location)


if __name__ == "__main__":
    unittest.main()
