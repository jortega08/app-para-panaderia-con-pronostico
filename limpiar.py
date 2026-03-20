from data.database import get_connection

with get_connection() as conn:
    conn.execute("DELETE FROM ventas")
    conn.execute("DELETE FROM registros_diarios")
    conn.execute("DELETE FROM pedidos")
    conn.execute("DELETE FROM pedido_items")
    conn.execute("DELETE FROM pedido_item_modificaciones")
    conn.commit()