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

    def _login_as_mesero(self) -> dict:
        response = self.client.post(
            "/login",
            data={
                "modo": "password",
                "username": "mesero",
                "password": "1111",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302, response.get_data(as_text=True))
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

    def test_public_cliente_pedido_page_renders(self) -> None:
        response = self.client.get("/cliente/pedido")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Tu pedido", response.get_data(as_text=True))

    def test_cierre_diario_api_responds_after_fresh_init(self) -> None:
        self._login_as_cajero()

        response = self.client.get("/api/cierre-diario")

        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("produccion", payload)

    def test_bulk_production_lots_use_tenant_unique_constraint(self) -> None:
        headers = self._login_as_cajero()

        first_response = self.client.post(
            "/api/produccion/lotes-masivos",
            json={
                "fecha": "2026-04-27",
                "lotes": [{"producto": "Pan Frances", "cantidad": 44}],
            },
            headers=headers,
        )
        self.assertEqual(first_response.status_code, 200, first_response.get_data(as_text=True))
        self.assertTrue(first_response.get_json()["ok"])

        second_response = self.client.post(
            "/api/produccion/lotes-masivos",
            json={
                "fecha": "2026-04-27",
                "lotes": [{"producto": "Pan Frances", "cantidad": 6}],
            },
            headers=headers,
        )
        self.assertEqual(second_response.status_code, 200, second_response.get_data(as_text=True))
        self.assertTrue(second_response.get_json()["ok"])

        with self.app_module.get_connection() as conn:
            row = conn.execute(
                """
                SELECT producido, panaderia_id, sede_id
                FROM registros_diarios
                WHERE fecha = ? AND producto = ?
                """,
                ("2026-04-27", "Pan Frances"),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(int(row["producido"]), 50)
        self.assertEqual(int(row["panaderia_id"]), 1)
        self.assertEqual(int(row["sede_id"]), 1)

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

    def test_generate_document_without_customer_uses_consumidor_final(self) -> None:
        headers = self._login_as_cajero()
        venta_id = self._start_sale(headers, tipo_venta="con_documento")
        self._attach_basic_items(venta_id, headers)

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
            json={"tipo_documento": "factura"},
            headers=headers,
        )
        self.assertEqual(document_response.status_code, 200, document_response.get_data(as_text=True))
        document_payload = document_response.get_json()
        self.assertTrue(document_payload["ok"])

        fetch_response = self.client.get(f"/api/documento/{document_payload['documento_id']}")
        self.assertEqual(fetch_response.status_code, 200, fetch_response.get_data(as_text=True))
        documento = fetch_response.get_json()["documento"]
        self.assertEqual(documento["cliente_nombre_snapshot"], "Consumidor final")
        self.assertEqual(documento["cliente_tipo_doc_snapshot"], "CC")
        self.assertEqual(documento["cliente_numero_doc_snapshot"], "2222222")
        self.assertIsNone(documento["cliente_id"])

        with self.app_module.get_connection() as conn:
            total_clientes = conn.execute(
                "SELECT COUNT(*) AS total FROM clientes WHERE numero_doc = ?",
                ("2222222",),
            ).fetchone()["total"]
        self.assertEqual(int(total_clientes), 0)

    def test_encargo_accepts_document_fields_custom_price_and_reminder(self) -> None:
        headers = self._login_as_cajero()

        response = self.client.post(
            "/api/encargo/v2",
            json={
                "cliente": "Cliente Encargo",
                "tipo_doc": "NIT",
                "numero_doc": "900222333",
                "email": "encargo@example.com",
                "empresa": "Cliente Encargo SAS",
                "fecha_entrega": "2030-05-01",
                "fecha_produccion": "2030-04-30",
                "recordatorio_entrega_en": "2030-04-30T14:00",
                "direccion_documento": "Calle Encargo 1",
                "anticipo": 9000,
                "items": [
                    {
                        "producto_id": 1,
                        "producto": "Pan Frances",
                        "cantidad": 1,
                        "precio_base": 5000,
                        "precio_aplicado": 9000,
                        "precio_unitario": 9000,
                        "motivo_precio": "Decoracion especial",
                    }
                ],
            },
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        payload = response.get_json()
        self.assertTrue(payload["ok"])

        detalle = self.client.get(f"/api/encargo/v2/{payload['encargo_id']}", headers=headers)
        self.assertEqual(detalle.status_code, 200, detalle.get_data(as_text=True))
        encargo = detalle.get_json()["encargo"]
        self.assertEqual(encargo["tipo_doc"], "NIT")
        self.assertEqual(encargo["numero_doc"], "900222333")
        self.assertEqual(encargo["fecha_produccion"], "2030-04-30")
        self.assertEqual(encargo["recordatorio_entrega_en"], "2030-04-30T14:00")
        self.assertEqual(float(encargo["total"]), 9000.0)
        self.assertEqual(float(encargo["items"][0]["precio_base"]), 5000.0)
        self.assertEqual(float(encargo["items"][0]["precio_aplicado"]), 9000.0)

    def test_order_delivery_checklist_and_table_attention_marker(self) -> None:
        headers = self._login_as_mesero()

        create_response = self.client.post(
            "/api/pedido",
            json={
                "mesa_id": 1,
                "items": [
                    {
                        "producto_id": 1,
                        "producto": "Pan Frances",
                        "cantidad": 2,
                    }
                ],
            },
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 200, create_response.get_data(as_text=True))
        pedido_id = int(create_response.get_json()["pedido_id"])

        pedido_response = self.client.get(f"/api/pedido/{pedido_id}", headers=headers)
        self.assertEqual(pedido_response.status_code, 200, pedido_response.get_data(as_text=True))
        pedido = pedido_response.get_json()
        item_id = int(pedido["items"][0]["id"])

        entrega_response = self.client.put(
            f"/api/pedido/{pedido_id}/items/{item_id}/entrega",
            json={"cantidad_entregada": 1},
            headers=headers,
        )
        self.assertEqual(entrega_response.status_code, 200, entrega_response.get_data(as_text=True))
        item_actualizado = entrega_response.get_json()["pedido"]["items"][0]
        self.assertEqual(int(item_actualizado["cantidad_entregada"]), 1)
        self.assertEqual(int(item_actualizado["cantidad_pendiente_entrega"]), 1)

        exceso_response = self.client.put(
            f"/api/pedido/{pedido_id}/items/{item_id}/entrega",
            json={"cantidad_entregada": 3},
            headers=headers,
        )
        self.assertEqual(exceso_response.status_code, 400)

        mesa_response = self.client.put(
            "/api/mesa/1/pendiente-atencion",
            json={"pendiente": True, "motivo": "Cliente pide la cuenta"},
            headers=headers,
        )
        self.assertEqual(mesa_response.status_code, 200, mesa_response.get_data(as_text=True))
        self.assertTrue(mesa_response.get_json()["mesa"]["pendiente_atencion"])

        mesas_page = self.client.get("/mesero/mesas")
        self.assertEqual(mesas_page.status_code, 200)
        self.assertIn("Por atender", mesas_page.get_data(as_text=True))

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
