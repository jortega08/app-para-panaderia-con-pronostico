from __future__ import annotations

import importlib
import gc
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _clear_runtime_modules() -> None:
    for name in list(sys.modules):
        if (
            name == "legacy_monolith_app"
            or name == "backup"
            or name == "app"
            or name.startswith("app.")
            or name == "data"
            or name.startswith("data.")
        ):
            sys.modules.pop(name, None)


def _load_app_module(db_path: Path):
    _clear_runtime_modules()
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    os.environ["ENABLE_IN_APP_SCHEDULER"] = "0"
    package = importlib.import_module("app")
    flask_app = package.create_app()
    flask_app.config.update(TESTING=True)
    return package._LEGACY_MODULE


class HardeningFlowsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "panaderia_test.db"
        self.app_module = _load_app_module(self.db_path)
        self.client = self.app_module.app.test_client()

    def tearDown(self) -> None:
        self.client = None
        self.app_module = None
        _clear_runtime_modules()
        gc.collect()
        for _ in range(5):
            try:
                self.temp_dir.cleanup()
                break
            except PermissionError:
                time.sleep(0.2)

    def _login_as_cajero(self) -> dict:
        response = self.client.post(
            "/login",
            data={
                "modo": "password",
                "username": "cajero",
                "password": "0000",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/cajero/pos", response.headers.get("Location", ""))
        with self.client.session_transaction() as session:
            csrf_token = session.get("_csrf_token") or session.get("csrf_token")
            self.assertTrue(csrf_token)
        return {"X-CSRF-Token": csrf_token}

    def _start_sale(self, headers: dict, tipo_venta: str = "rapida") -> int:
        response = self.client.post(
            "/api/venta/iniciar",
            json={"tipo_venta": tipo_venta},
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        return int(payload["venta_id"])

    def _attach_basic_items(self, venta_id: int, headers: dict) -> None:
        response = self.client.put(
            f"/api/venta/{venta_id}/items",
            json={
                "items": [
                    {
                        "producto": "Pan Frances",
                        "cantidad": 1,
                        "precio_base": 5000,
                        "precio_aplicado": 5000,
                    }
                ]
            },
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        self.assertTrue(response.get_json()["ok"])

    def test_login_password_flow_redirects_to_pos(self) -> None:
        headers = self._login_as_cajero()
        self.assertIn("X-CSRF-Token", headers)
        response = self.client.get("/cajero/pos")
        self.assertEqual(response.status_code, 200)
        self.assertIn("POS", response.get_data(as_text=True))

    def test_suspend_and_resume_sale_flow(self) -> None:
        headers = self._login_as_cajero()
        venta_id = self._start_sale(headers)
        self._attach_basic_items(venta_id, headers)

        suspend_response = self.client.post(
            f"/api/venta/{venta_id}/suspender",
            json={"nota": "Cliente pidio continuar luego"},
            headers=headers,
        )
        self.assertEqual(suspend_response.status_code, 200, suspend_response.get_data(as_text=True))
        self.assertTrue(suspend_response.get_json()["ok"])

        suspended_response = self.client.get("/api/ventas/suspendidas")
        self.assertEqual(suspended_response.status_code, 200)
        suspended_payload = suspended_response.get_json()
        self.assertTrue(any(int(venta["id"]) == venta_id for venta in suspended_payload["ventas"]))

        resume_response = self.client.post(
            f"/api/venta/{venta_id}/reanudar",
            json={},
            headers=headers,
        )
        self.assertEqual(resume_response.status_code, 200, resume_response.get_data(as_text=True))
        resume_payload = resume_response.get_json()
        self.assertTrue(resume_payload["ok"])
        self.assertEqual(resume_payload["venta"]["estado"], "activa")

    def test_generate_document_for_paid_sale(self) -> None:
        headers = self._login_as_cajero()
        venta_id = self._start_sale(headers, tipo_venta="con_documento")
        self._attach_basic_items(venta_id, headers)

        comprador_response = self.client.put(
            f"/api/venta/{venta_id}/comprador",
            json={
                "nombre_comprador": "Cliente Documento",
                "tipo_doc": "NIT",
                "numero_doc": "900123456",
                "email_comprador": "cliente@example.com",
                "empresa_comprador": "Cliente Documento SAS",
                "direccion_comprador": "Calle 123",
            },
            headers=headers,
        )
        self.assertEqual(comprador_response.status_code, 200)
        self.assertTrue(comprador_response.get_json()["ok"])

        pay_response = self.client.post(
            f"/api/venta/{venta_id}/pagar",
            json={"metodo": "efectivo", "monto": 5000, "recibido": 5000},
            headers=headers,
        )
        self.assertEqual(pay_response.status_code, 200, pay_response.get_data(as_text=True))
        self.assertTrue(pay_response.get_json()["ok"])

        close_response = self.client.post(
            f"/api/venta/{venta_id}/cerrar",
            json={},
            headers=headers,
        )
        self.assertEqual(close_response.status_code, 200, close_response.get_data(as_text=True))
        self.assertTrue(close_response.get_json()["ok"])

        document_response = self.client.post(
            f"/api/documento/generar-desde-venta/{venta_id}",
            json={
                "nombre": "Cliente Documento",
                "tipo_doc": "NIT",
                "numero_doc": "900123456",
                "email": "cliente@example.com",
                "empresa": "Cliente Documento SAS",
                "direccion": "Calle 123",
                "tipo_documento": "factura",
            },
            headers=headers,
        )
        self.assertEqual(document_response.status_code, 200, document_response.get_data(as_text=True))
        document_payload = document_response.get_json()
        self.assertTrue(document_payload["ok"])
        self.assertTrue(document_payload["consecutivo"].startswith("FAC-"))

        fetch_response = self.client.get(f"/api/documento/{document_payload['documento_id']}")
        self.assertEqual(fetch_response.status_code, 200)
        self.assertTrue(fetch_response.get_json()["ok"])

    def test_create_credit_and_register_abono(self) -> None:
        headers = self._login_as_cajero()
        cliente_response = self.client.post(
            "/api/clientes",
            json={
                "nombre": "Cliente Credito",
                "numero_doc": "900999888",
                "telefono": "3000000000",
            },
            headers=headers,
        )
        self.assertEqual(cliente_response.status_code, 200, cliente_response.get_data(as_text=True))
        cliente_payload = cliente_response.get_json()
        self.assertTrue(cliente_payload["ok"])
        cliente_id = int(cliente_payload["cliente_id"])

        venta_id = self._start_sale(headers)
        self._attach_basic_items(venta_id, headers)

        link_response = self.client.put(
            f"/api/venta/{venta_id}/cliente",
            json={
                "cliente_id": cliente_id,
                "cliente_nombre_snapshot": "Cliente Credito",
            },
            headers=headers,
        )
        self.assertEqual(link_response.status_code, 200)
        self.assertTrue(link_response.get_json()["ok"])

        pay_response = self.client.post(
            f"/api/venta/{venta_id}/pagar",
            json={"metodo": "credito", "monto": 5000},
            headers=headers,
        )
        self.assertEqual(pay_response.status_code, 200, pay_response.get_data(as_text=True))
        self.assertTrue(pay_response.get_json()["ok"])

        close_response = self.client.post(
            f"/api/venta/{venta_id}/cerrar",
            json={"fecha_vencimiento_credito": "2030-01-01"},
            headers=headers,
        )
        self.assertEqual(close_response.status_code, 200, close_response.get_data(as_text=True))
        close_payload = close_response.get_json()
        self.assertTrue(close_payload["ok"])
        cuenta_id = int(close_payload["cuenta_por_cobrar_id"])
        self.assertGreater(cuenta_id, 0)

        abono_response = self.client.post(
            f"/api/cartera/{cuenta_id}/abono",
            json={
                "monto": 2000,
                "metodo_pago": "efectivo",
                "referencia": "REC-TEST",
                "nota": "Abono inicial",
            },
            headers=headers,
        )
        self.assertEqual(abono_response.status_code, 200, abono_response.get_data(as_text=True))
        abono_payload = abono_response.get_json()
        self.assertTrue(abono_payload["ok"])
        self.assertEqual(abono_payload["estado"], "parcial")
        self.assertAlmostEqual(float(abono_payload["saldo_pendiente"]), 3000.0)

        detail_response = self.client.get(f"/api/cartera/{cuenta_id}")
        self.assertEqual(detail_response.status_code, 200)
        detail_payload = detail_response.get_json()
        self.assertTrue(detail_payload["ok"])
        self.assertEqual(len(detail_payload["cuenta"]["abonos"]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
