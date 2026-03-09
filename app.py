"""
app.py
------
Panaderia - Sistema de Ventas y Pronostico.
Interfaz moderna con tema claro, iconos, graficas y carrito POS.

Roles:
  - Panadero: pronosticos, produccion, configuracion
  - Cajero: punto de venta con carrito, resumen del dia
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.database import (
    inicializar_base_de_datos,
    guardar_registro,
    obtener_registros,
    obtener_productos,
    obtener_productos_con_precio,
    agregar_producto,
    actualizar_precio,
    verificar_pin,
    obtener_usuarios,
    agregar_usuario,
    eliminar_usuario,
    registrar_venta,
    obtener_ventas_dia,
    obtener_resumen_ventas_dia,
    obtener_total_ventas_dia,
    obtener_vendido_dia_producto,
)
from logic.pronostico import (
    calcular_pronostico,
    calcular_eficiencia,
    analizar_tendencia,
)

# Matplotlib (opcional)
try:
    import matplotlib
    matplotlib.use("TkAgg")
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    HAS_MPL = True
except ImportError:
    HAS_MPL = False


# ══════════════════════════════════════════════
# PALETA DE COLORES
# ══════════════════════════════════════════════
C = {
    # Fondos
    "bg":            "#FAF7F4",
    "bg_alt":        "#F0EBE3",
    "card":          "#FFFFFF",
    "card_hover":    "#FFF9F2",
    # Sidebar
    "sidebar":       "#2C1810",
    "sidebar_text":  "#D7CCC8",
    "sidebar_active":"#D4722A",
    "sidebar_hover": "#3E2723",
    # Colores principales
    "primary":       "#C17817",
    "primary_dark":  "#8B5513",
    "primary_light": "#F5DEB3",
    "accent":        "#D4722A",
    "accent_dark":   "#A0522D",
    # Texto
    "text":          "#1B0E07",
    "text_sec":      "#5D4037",
    "text_muted":    "#A1887F",
    # Estados
    "success":       "#388E3C",
    "success_bg":    "#E8F5E9",
    "warning":       "#EF6C00",
    "warning_bg":    "#FFF3E0",
    "danger":        "#D32F2F",
    "danger_bg":     "#FFEBEE",
    # Bordes
    "border":        "#D7CCC8",
    "divider":       "#EFEBE9",
    # Extras
    "selected":      "#FFE0B2",
    "topbar":        "#4E342E",
}

# ══════════════════════════════════════════════
# FUENTES
# ══════════════════════════════════════════════
F_HERO     = ("Segoe UI", 28, "bold")
F_TITULO   = ("Segoe UI", 22, "bold")
F_SUBTIT   = ("Segoe UI", 17, "bold")
F_GRANDE   = ("Segoe UI", 15)
F_GRANDE_B = ("Segoe UI", 15, "bold")
F_NORMAL   = ("Segoe UI", 13)
F_NORMAL_B = ("Segoe UI", 13, "bold")
F_BOTON    = ("Segoe UI", 14, "bold")
F_NUMERO   = ("Segoe UI", 36, "bold")
F_ICONO    = ("Segoe UI Emoji", 32)
F_ICONO_SM = ("Segoe UI Emoji", 20)
F_SMALL    = ("Segoe UI", 11)
F_SMALL_B  = ("Segoe UI", 11, "bold")

# ══════════════════════════════════════════════
# ICONOS POR PRODUCTO (emojis)
# ══════════════════════════════════════════════
ICONOS = {
    "Pan Frances":  "\U0001F956",   # baguette
    "Pan Dulce":    "\U0001F35E",   # bread
    "Croissant":    "\U0001F950",   # croissant
    "Integral":     "\U0001F95E",   # pancake/flatbread
}
ICONO_DEFAULT = "\U0001F9C1"        # cupcake

COLORES_PRODUCTO = {
    "Pan Frances":  "#E8B44D",
    "Pan Dulce":    "#E07A5F",
    "Croissant":    "#81B29A",
    "Integral":     "#9B8EA0",
}
COLOR_PROD_DEFAULT = "#B0BEC5"


def icono(producto: str) -> str:
    return ICONOS.get(producto, ICONO_DEFAULT)


def color_producto(producto: str) -> str:
    return COLORES_PRODUCTO.get(producto, COLOR_PROD_DEFAULT)


# ══════════════════════════════════════════════
# APLICACION PRINCIPAL
# ══════════════════════════════════════════════

class PanaderiaApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("Panaderia")
        self.geometry("1200x800")
        self.minsize(1000, 700)
        self.configure(bg=C["bg"])

        inicializar_base_de_datos()
        self.usuario_actual = None
        self._refresh_jobs = []
        self._setup_styles()
        self._mostrar_login()

    def _cancelar_refreshes(self):
        for job in self._refresh_jobs:
            try:
                self.after_cancel(job)
            except Exception:
                pass
        self._refresh_jobs.clear()

    def _setup_styles(self):
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TFrame", background=C["bg"])
        s.configure("Card.TFrame", background=C["card"])
        s.configure("TLabel", background=C["bg"], foreground=C["text"],
                     font=F_NORMAL)
        s.configure("Treeview", background=C["card"], foreground=C["text"],
                     fieldbackground=C["card"], font=F_NORMAL, rowheight=38)
        s.configure("Treeview.Heading", background=C["bg_alt"],
                     foreground=C["text_sec"], font=F_NORMAL_B)
        s.map("Treeview",
              background=[("selected", C["selected"])],
              foreground=[("selected", C["text"])])

    # ─────────── LOGIN ───────────

    def _mostrar_login(self):
        self._cancelar_refreshes()
        for w in self.winfo_children():
            w.destroy()
        self.usuario_actual = None

        # Fondo completo
        bg = tk.Frame(self, bg=C["bg"])
        bg.place(relwidth=1, relheight=1)

        # Panel central
        panel = tk.Frame(bg, bg=C["card"], padx=50, pady=40,
                          highlightbackground=C["border"],
                          highlightthickness=1)
        panel.place(relx=0.5, rely=0.5, anchor="center")

        # Header con color
        hdr = tk.Frame(panel, bg=C["accent"], height=6)
        hdr.pack(fill="x", pady=(0, 25))

        tk.Label(panel, text="\U0001F35E", font=("Segoe UI Emoji", 48),
                 bg=C["card"]).pack()
        tk.Label(panel, text="PANADERIA", font=("Segoe UI", 32, "bold"),
                 bg=C["card"], fg=C["primary_dark"]).pack(pady=(5, 0))
        tk.Label(panel, text="Sistema de Ventas y Pronostico",
                 font=F_GRANDE, bg=C["card"], fg=C["text_muted"]).pack()

        tk.Frame(panel, bg=C["divider"], height=1).pack(
            fill="x", pady=25, padx=20)

        tk.Label(panel, text="Ingresa tu PIN:", font=F_GRANDE_B,
                 bg=C["card"], fg=C["text"]).pack()

        # Campo PIN
        self.pin_var = tk.StringVar()
        self.pin_entry = tk.Entry(
            panel, textvariable=self.pin_var,
            font=("Segoe UI", 36, "bold"), width=6, justify="center",
            show="\u2022", bg=C["bg"], fg=C["text"],
            insertbackground=C["accent"],
            relief="flat", bd=0,
            highlightthickness=2, highlightcolor=C["accent"],
            highlightbackground=C["border"]
        )
        self.pin_entry.pack(pady=15, ipady=8)
        self.pin_entry.focus_set()
        self.pin_entry.bind("<Return>", lambda _: self._intentar_login())

        # Teclado numerico
        teclado = tk.Frame(panel, bg=C["card"])
        teclado.pack(pady=10)

        keys = [
            ["1", "2", "3"],
            ["4", "5", "6"],
            ["7", "8", "9"],
            ["\u232B", "0", "\u23CE"],
        ]
        for fila in keys:
            row = tk.Frame(teclado, bg=C["card"])
            row.pack()
            for k in fila:
                if k == "\u23CE":  # Enter
                    btn = tk.Button(
                        row, text=k, font=("Segoe UI", 20), width=5, height=1,
                        bg=C["success"], fg="white", relief="flat",
                        activebackground="#2E7D32", cursor="hand2",
                        command=self._intentar_login)
                elif k == "\u232B":  # Backspace
                    btn = tk.Button(
                        row, text=k, font=("Segoe UI", 20), width=5, height=1,
                        bg=C["danger_bg"], fg=C["danger"], relief="flat",
                        activebackground="#FFCDD2", cursor="hand2",
                        command=lambda: self.pin_var.set(
                            self.pin_var.get()[:-1]))
                else:
                    btn = tk.Button(
                        row, text=k, font=("Segoe UI", 20, "bold"),
                        width=5, height=1,
                        bg=C["bg"], fg=C["text"], relief="flat",
                        activebackground=C["selected"], cursor="hand2",
                        command=lambda n=k: self.pin_var.set(
                            self.pin_var.get() + n))
                btn.pack(side="left", padx=3, pady=3)

        self.login_msg = tk.Label(panel, text="", font=F_NORMAL,
                                   bg=C["card"], fg=C["danger"])
        self.login_msg.pack(pady=8)

        tk.Label(panel, text="Panadero: 1234  |  Cajero: 0000",
                 font=F_SMALL, bg=C["card"],
                 fg=C["text_muted"]).pack(pady=(5, 0))

    def _intentar_login(self):
        pin = self.pin_var.get().strip()
        if not pin:
            self.login_msg.configure(text="Escribe tu PIN")
            return
        usuario = verificar_pin(pin)
        if usuario:
            self.usuario_actual = usuario
            self._cancelar_refreshes()
            for w in self.winfo_children():
                w.destroy()
            if usuario["rol"] == "cajero":
                self._construir_cajero()
            else:
                self._construir_panadero()
        else:
            self.login_msg.configure(text="PIN incorrecto")
            self.pin_var.set("")

    # ─────────── CAJERO ───────────

    def _construir_cajero(self):
        # Top bar
        top = tk.Frame(self, bg=C["accent"], height=56)
        top.pack(fill="x")
        top.pack_propagate(False)

        tk.Label(top, text=f"\U0001F9D1\u200D\U0001F373  {self.usuario_actual['nombre']}",
                 font=F_GRANDE_B, bg=C["accent"], fg="white"
                 ).pack(side="left", padx=20)

        tk.Button(top, text="Salir", font=F_NORMAL_B,
                  bg=C["accent_dark"], fg="white", relief="flat",
                  activebackground=C["danger"], cursor="hand2",
                  padx=15, command=self._mostrar_login
                  ).pack(side="right", padx=15, pady=10)

        # Tab bar
        tab_bar = tk.Frame(self, bg=C["card"], height=50)
        tab_bar.pack(fill="x")
        tab_bar.pack_propagate(False)

        # Bottom accent line
        tk.Frame(tab_bar, bg=C["divider"], height=1).pack(
            side="bottom", fill="x")

        self._cajero_tabs = {}
        self._cajero_content = tk.Frame(self, bg=C["bg"])
        self._cajero_content.pack(fill="both", expand=True)

        for texto, vid in [("\U0001F6D2  Punto de Venta", "pos"),
                            ("\U0001F4CA  Ventas de Hoy", "resumen")]:
            btn = tk.Button(
                tab_bar, text=texto, font=F_NORMAL_B,
                bg=C["card"], fg=C["text_sec"], relief="flat",
                activebackground=C["selected"], cursor="hand2", padx=20,
                command=lambda v=vid: self._cajero_vista(v))
            btn.pack(side="left", padx=2, pady=6)
            self._cajero_tabs[vid] = btn

        self._cajero_vista("pos")

    def _cajero_vista(self, nombre):
        self._cancelar_refreshes()
        for w in self._cajero_content.winfo_children():
            w.destroy()
        for k, btn in self._cajero_tabs.items():
            if k == nombre:
                btn.configure(bg=C["accent"], fg="white")
            else:
                btn.configure(bg=C["card"], fg=C["text_sec"])

        if nombre == "pos":
            VistaPOS(self._cajero_content, self).pack(
                fill="both", expand=True)
        else:
            VistaVentasDashboard(self._cajero_content, self).pack(
                fill="both", expand=True)

    # ─────────── PANADERO ───────────

    def _construir_panadero(self):
        body = tk.Frame(self, bg=C["bg"])
        body.pack(fill="both", expand=True)

        # Sidebar oscuro
        sidebar = tk.Frame(body, bg=C["sidebar"], width=230)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)

        # Logo en sidebar
        tk.Label(sidebar, text="\U0001F35E", font=("Segoe UI Emoji", 36),
                 bg=C["sidebar"]).pack(pady=(25, 5))
        tk.Label(sidebar, text="PANADERIA", font=("Segoe UI", 16, "bold"),
                 bg=C["sidebar"], fg="white").pack()
        tk.Label(sidebar, text=self.usuario_actual["nombre"],
                 font=F_SMALL, bg=C["sidebar"],
                 fg=C["sidebar_text"]).pack(pady=(2, 20))

        tk.Frame(sidebar, bg=C["sidebar_hover"], height=1).pack(
            fill="x", padx=15)

        self._pan_tabs = {}
        self._pan_content = tk.Frame(body, bg=C["bg"])
        self._pan_content.pack(side="left", fill="both", expand=True)

        items = [
            ("\U0001F4CB  Cuantos Hornear", "pronostico"),
            ("\U0001F525  Registrar Produccion", "produccion"),
            ("\U0001F4B0  Ventas de Hoy", "ventas"),
            ("\U0001F4C5  Historial", "historial"),
            ("\u2699\uFE0F  Configuracion", "config"),
        ]
        for texto, vid in items:
            btn = tk.Button(
                sidebar, text=texto, font=F_NORMAL, anchor="w",
                bg=C["sidebar"], fg=C["sidebar_text"], relief="flat",
                activebackground=C["sidebar_hover"],
                activeforeground="white", cursor="hand2",
                padx=20, pady=12,
                command=lambda v=vid: self._panadero_vista(v))
            btn.pack(fill="x", pady=1)
            self._pan_tabs[vid] = btn

        # Fecha y boton salir abajo
        bottom = tk.Frame(sidebar, bg=C["sidebar"])
        bottom.pack(side="bottom", fill="x", padx=15, pady=15)

        tk.Label(bottom,
                 text=datetime.now().strftime("%d/%m/%Y"),
                 font=F_NORMAL, bg=C["sidebar"],
                 fg=C["sidebar_text"]).pack(pady=(0, 10))

        tk.Button(bottom, text="Cerrar Sesion", font=F_SMALL_B,
                  bg=C["sidebar_hover"], fg=C["sidebar_text"],
                  relief="flat", activebackground=C["danger"],
                  cursor="hand2", command=self._mostrar_login
                  ).pack(fill="x")

        self._panadero_vista("pronostico")

    def _panadero_vista(self, nombre):
        self._cancelar_refreshes()
        for w in self._pan_content.winfo_children():
            w.destroy()
        for k, btn in self._pan_tabs.items():
            if k == nombre:
                btn.configure(bg=C["sidebar_active"], fg="white")
            else:
                btn.configure(bg=C["sidebar"], fg=C["sidebar_text"])

        vistas = {
            "pronostico": VistaPronostico,
            "produccion": VistaProduccion,
            "ventas":     VistaVentasDashboard,
            "historial":  VistaHistorial,
            "config":     VistaConfiguracion,
        }
        if nombre in vistas:
            vistas[nombre](self._pan_content, self).pack(
                fill="both", expand=True)


# ══════════════════════════════════════════════
# PUNTO DE VENTA CON CARRITO
# ══════════════════════════════════════════════

class VistaPOS(tk.Frame):
    """POS con carrito: selecciona productos, acumula pedido, cobra."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, bg=C["bg"])
        self.app = app
        self.carrito = []  # [{producto, precio, cantidad}]
        self._construir()

    def _construir(self):
        # Titulo
        hdr = tk.Frame(self, bg=C["bg"])
        hdr.pack(fill="x", padx=25, pady=(18, 8))
        tk.Label(hdr, text="Punto de Venta", font=F_TITULO,
                 bg=C["bg"], fg=C["text"]).pack(side="left")

        # Contenido principal
        main = tk.Frame(self, bg=C["bg"])
        main.pack(fill="both", expand=True, padx=25, pady=(0, 15))

        # ── Panel izquierdo: productos ──
        left = tk.Frame(main, bg=C["bg"])
        left.pack(side="left", fill="both", expand=True, padx=(0, 15))

        tk.Label(left, text="Toca un producto para agregarlo al pedido:",
                 font=F_NORMAL, bg=C["bg"],
                 fg=C["text_muted"]).pack(anchor="w", pady=(0, 10))

        grid = tk.Frame(left, bg=C["bg"])
        grid.pack(fill="both", expand=True)

        productos = obtener_productos_con_precio()
        for i, p in enumerate(productos):
            col_prod = color_producto(p["nombre"])
            fila, col = i // 2, i % 2
            grid.grid_columnconfigure(col, weight=1)
            grid.grid_rowconfigure(fila, weight=1)

            card = tk.Frame(grid, bg=C["card"],
                             highlightbackground=C["border"],
                             highlightthickness=1, cursor="hand2")
            card.grid(row=fila, column=col, padx=8, pady=8, sticky="nsew")

            # Barra de color superior
            tk.Frame(card, bg=col_prod, height=5).pack(fill="x")

            # Contenido
            inner = tk.Frame(card, bg=C["card"], padx=15, pady=12)
            inner.pack(fill="both", expand=True)

            tk.Label(inner, text=icono(p["nombre"]),
                     font=F_ICONO, bg=C["card"]).pack(pady=(5, 2))

            tk.Label(inner, text=p["nombre"], font=F_GRANDE_B,
                     bg=C["card"], fg=C["text"]).pack()

            tk.Label(inner, text=f"${p['precio']:.2f}",
                     font=("Segoe UI", 18, "bold"),
                     bg=C["card"], fg=C["primary"]).pack(pady=(3, 5))

            # Bind click a toda la tarjeta y sus hijos
            for widget in [card, inner] + inner.winfo_children():
                widget.bind("<Button-1>",
                            lambda e, prod=p: self._agregar_al_carrito(prod))

        # ── Panel derecho: carrito ──
        right = tk.Frame(main, bg=C["card"], width=360,
                          highlightbackground=C["border"],
                          highlightthickness=1)
        right.pack(side="right", fill="y")
        right.pack_propagate(False)

        # Header carrito
        cart_hdr = tk.Frame(right, bg=C["primary_dark"], padx=15, pady=10)
        cart_hdr.pack(fill="x")
        tk.Label(cart_hdr, text="\U0001F6D2  Pedido Actual",
                 font=F_GRANDE_B, bg=C["primary_dark"],
                 fg="white").pack(side="left")
        self.lbl_items = tk.Label(cart_hdr, text="0 items",
                                   font=F_SMALL, bg=C["primary_dark"],
                                   fg=C["primary_light"])
        self.lbl_items.pack(side="right")

        # Lista de items del carrito
        self.carrito_frame = tk.Frame(right, bg=C["card"])
        self.carrito_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.carrito_vacio_lbl = tk.Label(
            self.carrito_frame,
            text="El pedido esta vacio.\n\nToca un producto\npara agregarlo.",
            font=F_NORMAL, bg=C["card"], fg=C["text_muted"], justify="center")
        self.carrito_vacio_lbl.pack(expand=True)

        # Footer: total y botones
        footer = tk.Frame(right, bg=C["card"], padx=15, pady=10)
        footer.pack(fill="x", side="bottom")

        tk.Frame(footer, bg=C["divider"], height=1).pack(fill="x", pady=(0, 10))

        total_row = tk.Frame(footer, bg=C["card"])
        total_row.pack(fill="x")
        tk.Label(total_row, text="TOTAL:", font=F_SUBTIT,
                 bg=C["card"], fg=C["text_sec"]).pack(side="left")
        self.lbl_total = tk.Label(total_row, text="$0.00",
                                   font=("Segoe UI", 26, "bold"),
                                   bg=C["card"], fg=C["success"])
        self.lbl_total.pack(side="right")

        btn_row = tk.Frame(footer, bg=C["card"])
        btn_row.pack(fill="x", pady=(12, 0))

        tk.Button(btn_row, text="Limpiar", font=F_NORMAL_B,
                  bg=C["danger_bg"], fg=C["danger"], relief="flat",
                  activebackground="#FFCDD2", cursor="hand2",
                  padx=15, pady=8,
                  command=self._limpiar_carrito).pack(side="left")

        tk.Button(btn_row, text="\u2714  Cobrar", font=F_BOTON,
                  bg=C["success"], fg="white", relief="flat",
                  activebackground="#2E7D32", cursor="hand2",
                  padx=25, pady=10,
                  command=self._cobrar).pack(side="right")

    def _agregar_al_carrito(self, producto):
        # Buscar si ya esta en el carrito
        for item in self.carrito:
            if item["producto"] == producto["nombre"]:
                item["cantidad"] += 1
                self._render_carrito()
                return
        self.carrito.append({
            "producto": producto["nombre"],
            "precio": producto["precio"],
            "cantidad": 1,
        })
        self._render_carrito()

    def _render_carrito(self):
        for w in self.carrito_frame.winfo_children():
            w.destroy()

        if not self.carrito:
            self.carrito_vacio_lbl = tk.Label(
                self.carrito_frame,
                text="El pedido esta vacio.\n\nToca un producto\npara agregarlo.",
                font=F_NORMAL, bg=C["card"], fg=C["text_muted"],
                justify="center")
            self.carrito_vacio_lbl.pack(expand=True)
            self.lbl_total.configure(text="$0.00")
            self.lbl_items.configure(text="0 items")
            return

        total = 0
        total_items = 0

        for item in self.carrito:
            subtotal = item["precio"] * item["cantidad"]
            total += subtotal
            total_items += item["cantidad"]

            row = tk.Frame(self.carrito_frame, bg=C["card"], pady=6)
            row.pack(fill="x")

            # Icono + nombre
            col_p = color_producto(item["producto"])
            tk.Label(row, text=icono(item["producto"]),
                     font=F_ICONO_SM, bg=C["card"]).pack(side="left")

            info = tk.Frame(row, bg=C["card"])
            info.pack(side="left", padx=8, fill="x", expand=True)

            tk.Label(info, text=item["producto"], font=F_NORMAL_B,
                     bg=C["card"], fg=C["text"]).pack(anchor="w")
            tk.Label(info, text=f"${item['precio']:.2f} c/u",
                     font=F_SMALL, bg=C["card"],
                     fg=C["text_muted"]).pack(anchor="w")

            # Controles cantidad
            ctrl = tk.Frame(row, bg=C["card"])
            ctrl.pack(side="right")

            tk.Button(ctrl, text="-", font=("Segoe UI", 12, "bold"),
                      bg=C["danger_bg"], fg=C["danger"], relief="flat",
                      width=2, cursor="hand2",
                      command=lambda it=item: self._cambiar_cant(it, -1)
                      ).pack(side="left", padx=1)

            tk.Label(ctrl, text=str(item["cantidad"]),
                     font=F_NORMAL_B, bg=C["card"], fg=C["text"],
                     width=3).pack(side="left")

            tk.Button(ctrl, text="+", font=("Segoe UI", 12, "bold"),
                      bg=C["success_bg"], fg=C["success"], relief="flat",
                      width=2, cursor="hand2",
                      command=lambda it=item: self._cambiar_cant(it, 1)
                      ).pack(side="left", padx=1)

            # Subtotal
            tk.Label(ctrl, text=f"${subtotal:.2f}", font=F_NORMAL_B,
                     bg=C["card"], fg=C["primary"],
                     width=8, anchor="e").pack(side="left", padx=(8, 0))

            # Separador
            tk.Frame(self.carrito_frame, bg=C["divider"],
                     height=1).pack(fill="x")

        self.lbl_total.configure(text=f"${total:.2f}")
        self.lbl_items.configure(
            text=f"{total_items} {'item' if total_items == 1 else 'items'}")

    def _cambiar_cant(self, item, delta):
        item["cantidad"] += delta
        if item["cantidad"] <= 0:
            self.carrito.remove(item)
        self._render_carrito()

    def _limpiar_carrito(self):
        self.carrito.clear()
        self._render_carrito()

    def _cobrar(self):
        if not self.carrito:
            messagebox.showinfo("Pedido vacio",
                                "Agrega productos al pedido primero.")
            return

        usuario = self.app.usuario_actual["nombre"]
        total = 0
        detalle = []
        errores = []

        for item in self.carrito:
            subtotal = item["precio"] * item["cantidad"]
            total += subtotal
            ok = registrar_venta(item["producto"], item["cantidad"],
                                 item["precio"], usuario)
            if ok:
                detalle.append(
                    f"  {icono(item['producto'])} {item['cantidad']}x "
                    f"{item['producto']} = ${subtotal:.2f}")
            else:
                errores.append(item["producto"])

        if errores:
            messagebox.showwarning(
                "Atencion",
                f"No se pudieron registrar: {', '.join(errores)}\n"
                "Los demas si se guardaron.")
        else:
            messagebox.showinfo(
                "Venta Registrada",
                f"Pedido cobrado:\n\n"
                + "\n".join(detalle) +
                f"\n\nTotal: ${total:.2f}")

        self.carrito.clear()
        self._render_carrito()


# ══════════════════════════════════════════════
# DASHBOARD DE VENTAS (cajero y panadero)
# ══════════════════════════════════════════════

class VistaVentasDashboard(tk.Frame):
    """Dashboard de ventas del dia con metricas, grafica y tabla."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, bg=C["bg"])
        self.app = app
        self._chart_canvas = None
        self._construir()
        self._iniciar_autorefresh()

    def _construir(self):
        # Scroll
        self.canvas_scroll = tk.Canvas(self, bg=C["bg"],
                                        highlightthickness=0)
        sb = ttk.Scrollbar(self, orient="vertical",
                            command=self.canvas_scroll.yview)
        self.scroll_frame = tk.Frame(self.canvas_scroll, bg=C["bg"])

        self.scroll_frame.bind(
            "<Configure>",
            lambda e: self.canvas_scroll.configure(
                scrollregion=self.canvas_scroll.bbox("all")))
        self.canvas_scroll.create_window((0, 0), window=self.scroll_frame,
                                          anchor="nw")
        self.canvas_scroll.configure(yscrollcommand=sb.set)
        self.canvas_scroll.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        _bind_scroll(self.canvas_scroll)

        self.contenido = self.scroll_frame
        self._cargar_datos()

    def _cargar_datos(self):
        for w in self.contenido.winfo_children():
            w.destroy()

        # Header
        hdr = tk.Frame(self.contenido, bg=C["bg"])
        hdr.pack(fill="x", padx=25, pady=(18, 5))

        tk.Label(hdr, text="Ventas de Hoy", font=F_TITULO,
                 bg=C["bg"], fg=C["text"]).pack(side="left")

        ahora = datetime.now().strftime("%H:%M")
        self.lbl_reloj = tk.Label(
            hdr, text=f"Actualizado: {ahora}", font=F_SMALL,
            bg=C["bg"], fg=C["text_muted"])
        self.lbl_reloj.pack(side="right")

        tk.Button(hdr, text="\u21BB  Actualizar", font=F_NORMAL_B,
                  bg=C["accent"], fg="white", relief="flat",
                  activebackground=C["accent_dark"], cursor="hand2",
                  padx=12, pady=4,
                  command=self._cargar_datos).pack(side="right", padx=10)

        try:
            totales = obtener_total_ventas_dia()
            resumen = obtener_resumen_ventas_dia()
            ventas = obtener_ventas_dia()
        except Exception as e:
            tk.Label(self.contenido,
                     text=f"Error al cargar datos: {e}",
                     font=F_GRANDE, bg=C["bg"],
                     fg=C["danger"]).pack(pady=30, padx=25)
            return

        # Tarjetas metricas
        cards_row = tk.Frame(self.contenido, bg=C["bg"])
        cards_row.pack(fill="x", padx=25, pady=12)

        _metrica_card(cards_row, "Total Vendido",
                      f"${totales['dinero']:.2f}",
                      C["success"], "\U0001F4B5")
        _metrica_card(cards_row, "Panes Vendidos",
                      str(totales["panes"]),
                      C["primary"], "\U0001F35E")
        _metrica_card(cards_row, "Transacciones",
                      str(totales["transacciones"]),
                      C["accent"], "\U0001F9FE")

        # Grafica + tabla lado a lado
        content_row = tk.Frame(self.contenido, bg=C["bg"])
        content_row.pack(fill="both", expand=True, padx=25, pady=5)

        # ── Grafica de barras ──
        if resumen and HAS_MPL:
            chart_card = _card(content_row, "Ventas por Producto")
            chart_card.pack(side="left", fill="both", expand=True,
                            padx=(0, 10))

            nombres = [r["producto"] for r in resumen]
            cantidades = [r["total_cantidad"] for r in resumen]
            colores = [color_producto(n) for n in nombres]

            fig = Figure(figsize=(5, 3.2), dpi=100)
            fig.patch.set_facecolor(C["card"])
            ax = fig.add_subplot(111)
            ax.set_facecolor(C["card"])

            bars = ax.barh(nombres, cantidades, color=colores,
                           height=0.6, edgecolor="white", linewidth=1.5)
            ax.set_xlabel("Cantidad", fontsize=10, color=C["text_sec"])
            ax.tick_params(colors=C["text_sec"], labelsize=10)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.spines["left"].set_color(C["divider"])
            ax.spines["bottom"].set_color(C["divider"])

            # Valores en las barras
            for bar, cant in zip(bars, cantidades):
                ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                        str(cant), va="center", fontsize=11,
                        fontweight="bold", color=C["text_sec"])

            fig.tight_layout(pad=2)
            self._chart_canvas = FigureCanvasTkAgg(fig, chart_card)
            self._chart_canvas.draw()
            self._chart_canvas.get_tk_widget().pack(fill="both", expand=True)

        # ── Resumen por producto (tarjetas) ──
        detail_card = _card(content_row, "Detalle por Producto")
        detail_card.pack(side="left", fill="both", expand=True)

        if resumen:
            for r in resumen:
                row = tk.Frame(detail_card, bg=C["card"], pady=8)
                row.pack(fill="x")

                col_p = color_producto(r["producto"])
                # Color dot
                dot = tk.Canvas(row, width=14, height=14,
                                bg=C["card"], highlightthickness=0)
                dot.create_oval(2, 2, 12, 12, fill=col_p, outline=col_p)
                dot.pack(side="left", padx=(0, 8))

                tk.Label(row, text=f"{icono(r['producto'])}  {r['producto']}",
                         font=F_NORMAL_B, bg=C["card"],
                         fg=C["text"]).pack(side="left")

                tk.Label(row, text=f"${r['total_dinero']:.2f}",
                         font=F_GRANDE_B, bg=C["card"],
                         fg=C["success"]).pack(side="right")

                tk.Label(row, text=f"{r['total_cantidad']} panes  |  ",
                         font=F_NORMAL, bg=C["card"],
                         fg=C["text_muted"]).pack(side="right")

                tk.Frame(detail_card, bg=C["divider"],
                         height=1).pack(fill="x")
        else:
            tk.Label(detail_card,
                     text="No hay ventas registradas hoy.\n\n"
                          "Las ventas apareceran aqui cuando\n"
                          "el cajero registre pedidos.",
                     font=F_NORMAL, bg=C["card"],
                     fg=C["text_muted"], justify="center").pack(
                         expand=True, pady=30)

        # ── Ultimas transacciones ──
        if ventas:
            trans_card = _card(self.contenido, "Ultimas Transacciones")
            trans_card.pack(fill="x", padx=25, pady=(10, 20))

            cols = ("hora", "producto", "cantidad", "total")
            tabla = ttk.Treeview(trans_card, columns=cols,
                                  show="headings", height=min(8, len(ventas)))

            for col, titulo, ancho in [
                ("hora", "Hora", 80),
                ("producto", "Producto", 200),
                ("cantidad", "Cantidad", 100),
                ("total", "Total", 120),
            ]:
                tabla.heading(col, text=titulo)
                tabla.column(col, width=ancho,
                             anchor="center" if col != "producto" else "w")

            for v in ventas[:20]:
                tabla.insert("", "end", values=(
                    v["hora"][:5],
                    f"{icono(v['producto'])}  {v['producto']}",
                    v["cantidad"],
                    f"${v['total']:.2f}"
                ))

            tabla.pack(fill="x", pady=(5, 0))

    def _iniciar_autorefresh(self):
        """Refresca los datos cada 30 segundos."""
        def refresh():
            try:
                self._cargar_datos()
            except Exception:
                pass
            job = self.app.after(30000, refresh)
            self.app._refresh_jobs.append(job)

        job = self.app.after(30000, refresh)
        self.app._refresh_jobs.append(job)


# ══════════════════════════════════════════════
# PRONOSTICO CON GRAFICAS
# ══════════════════════════════════════════════

class VistaPronostico(tk.Frame):
    """Cuantos hornear hoy - con grafica de barras."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, bg=C["bg"])
        self.app = app
        self._construir()

    def _construir(self):
        canvas_s = tk.Canvas(self, bg=C["bg"], highlightthickness=0)
        sb = ttk.Scrollbar(self, orient="vertical", command=canvas_s.yview)
        scroll = tk.Frame(canvas_s, bg=C["bg"])

        scroll.bind("<Configure>",
                     lambda e: canvas_s.configure(
                         scrollregion=canvas_s.bbox("all")))
        canvas_s.create_window((0, 0), window=scroll, anchor="nw")
        canvas_s.configure(yscrollcommand=sb.set)
        canvas_s.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")
        _bind_scroll(canvas_s)

        # Header
        hdr = tk.Frame(scroll, bg=C["bg"])
        hdr.pack(fill="x", padx=25, pady=(18, 5))

        tk.Label(hdr, text="\U0001F4CB  Cuantos Hornear Hoy",
                 font=F_TITULO, bg=C["bg"], fg=C["text"]).pack(side="left")

        fecha_hoy = datetime.now().strftime("%A %d/%m/%Y").capitalize()
        tk.Label(hdr, text=fecha_hoy, font=F_NORMAL,
                 bg=C["bg"], fg=C["text_muted"]).pack(side="right")

        productos = obtener_productos()
        if not productos:
            tk.Label(scroll,
                     text="No hay productos configurados.\n"
                          "Ve a Configuracion para agregar productos.",
                     font=F_GRANDE, bg=C["bg"],
                     fg=C["text_muted"]).pack(pady=50)
            return

        # Calcular pronosticos
        pronosticos = []
        for p in productos:
            try:
                r = calcular_pronostico(p)
                pronosticos.append(r)
            except Exception as e:
                pronosticos.append(None)

        # ── Grafica resumen ──
        if HAS_MPL and pronosticos:
            validos = [(p, r) for p, r in zip(productos, pronosticos) if r]
            if validos:
                chart_frame = _card(scroll, "Produccion Sugerida")
                chart_frame.pack(fill="x", padx=25, pady=(10, 5))

                nombres = [p for p, _ in validos]
                sugeridos = [r.produccion_sugerida for _, r in validos]
                promedios = [r.promedio_ventas for _, r in validos]
                colores = [color_producto(p) for p in nombres]

                fig = Figure(figsize=(8, 3), dpi=100)
                fig.patch.set_facecolor(C["card"])
                ax = fig.add_subplot(111)
                ax.set_facecolor(C["card"])

                x = range(len(nombres))
                w = 0.35
                bars1 = ax.bar([i - w/2 for i in x], promedios, w,
                               label="Promedio vendido",
                               color=[c + "80" for c in colores],
                               edgecolor="white")
                bars2 = ax.bar([i + w/2 for i in x], sugeridos, w,
                               label="Sugerido hornear",
                               color=colores, edgecolor="white")

                ax.set_xticks(list(x))
                ax.set_xticklabels([f"{icono(n)} {n}" for n in nombres],
                                    fontsize=10)
                ax.set_ylabel("Panes", fontsize=10, color=C["text_sec"])
                ax.legend(fontsize=9, loc="upper right")
                ax.tick_params(colors=C["text_sec"])
                ax.spines["top"].set_visible(False)
                ax.spines["right"].set_visible(False)
                ax.spines["left"].set_color(C["divider"])
                ax.spines["bottom"].set_color(C["divider"])

                # Valores encima de barras
                for bar in bars2:
                    ax.text(bar.get_x() + bar.get_width()/2,
                            bar.get_height() + 1,
                            str(int(bar.get_height())),
                            ha="center", fontsize=10, fontweight="bold",
                            color=C["text_sec"])

                fig.tight_layout(pad=2)
                chart_canvas = FigureCanvasTkAgg(fig, chart_frame)
                chart_canvas.draw()
                chart_canvas.get_tk_widget().pack(fill="x")

        # ── Tarjetas por producto ──
        for p, resultado in zip(productos, pronosticos):
            if resultado is None:
                continue

            registros = obtener_registros(p, dias=7)
            eficiencia = calcular_eficiencia(registros)
            tendencia = analizar_tendencia(registros)

            color_map = {
                "bien": C["success"],
                "alerta": C["warning"],
                "problema": C["danger"],
            }
            bg_map = {
                "bien": C["success_bg"],
                "alerta": C["warning_bg"],
                "problema": C["danger_bg"],
            }
            estado_c = color_map.get(resultado.estado, C["text_muted"])
            estado_bg = bg_map.get(resultado.estado, C["bg"])
            col_p = color_producto(p)

            # Card
            outer = tk.Frame(scroll, bg=C["card"], padx=0, pady=0,
                              highlightbackground=C["border"],
                              highlightthickness=1)
            outer.pack(fill="x", padx=25, pady=6)

            # Color bar lateral
            inner = tk.Frame(outer, bg=C["card"])
            inner.pack(fill="x", side="right", expand=True)

            bar = tk.Frame(outer, bg=col_p, width=6)
            bar.pack(side="left", fill="y")

            content = tk.Frame(inner, bg=C["card"], padx=20, pady=14)
            content.pack(fill="x")

            # Fila 1: Icono + Nombre + Numero grande
            row1 = tk.Frame(content, bg=C["card"])
            row1.pack(fill="x")

            tk.Label(row1, text=f"{icono(p)}  {p}",
                     font=F_SUBTIT, bg=C["card"],
                     fg=C["text"]).pack(side="left")

            num_box = tk.Frame(row1, bg=estado_bg, padx=16, pady=6)
            num_box.pack(side="right")
            tk.Label(num_box,
                     text=f"{resultado.produccion_sugerida} panes",
                     font=("Segoe UI", 22, "bold"),
                     bg=estado_bg, fg=estado_c).pack()

            # Fila 2: Detalles
            row2 = tk.Frame(content, bg=C["card"])
            row2.pack(fill="x", pady=(8, 0))

            detalles = [
                ("Promedio", f"{resultado.promedio_ventas}/dia"),
                ("Tendencia", tendencia),
                ("Datos", f"{resultado.dias_historial} dias"),
                ("Confianza", resultado.confianza),
            ]
            if eficiencia:
                detalles.insert(2, (
                    "Aprovechamiento",
                    f"{eficiencia.get('tasa_aprovechamiento', 0)}%"))

            for label, valor in detalles:
                chip = tk.Frame(row2, bg=C["bg_alt"], padx=8, pady=3)
                chip.pack(side="left", padx=(0, 6))
                tk.Label(chip, text=f"{label}: {valor}",
                         font=F_SMALL, bg=C["bg_alt"],
                         fg=C["text_sec"]).pack()

            # Fila 3: Mensaje
            row3 = tk.Frame(content, bg=estado_bg, padx=10, pady=5)
            row3.pack(fill="x", pady=(8, 0))
            tk.Label(row3, text=resultado.mensaje,
                     font=F_NORMAL, bg=estado_bg,
                     fg=estado_c).pack(anchor="w")


# ══════════════════════════════════════════════
# REGISTRAR PRODUCCION
# ══════════════════════════════════════════════

class VistaProduccion(tk.Frame):

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, bg=C["bg"])
        self.app = app
        self._construir()

    def _construir(self):
        hdr = tk.Frame(self, bg=C["bg"])
        hdr.pack(fill="x", padx=25, pady=(18, 5))
        tk.Label(hdr, text="\U0001F525  Registrar Produccion",
                 font=F_TITULO, bg=C["bg"], fg=C["text"]).pack(side="left")

        tk.Label(self, text="Cuantos panes se hornearon hoy?",
                 font=F_NORMAL, bg=C["bg"],
                 fg=C["text_muted"]).pack(padx=25, anchor="w", pady=(0, 10))

        # Formulario card
        form_wrap = tk.Frame(self, bg=C["bg"])
        form_wrap.pack(expand=True)

        form = tk.Frame(form_wrap, bg=C["card"], padx=35, pady=30,
                         highlightbackground=C["border"],
                         highlightthickness=1)
        form.pack(padx=20)

        # Fecha
        r = 0
        tk.Label(form, text="Fecha:", font=F_NORMAL_B,
                 bg=C["card"], fg=C["text_sec"]).grid(
                     row=r, column=0, sticky="w", pady=10, padx=(0, 20))
        self.fecha_var = tk.StringVar(
            value=datetime.now().strftime("%Y-%m-%d"))
        _styled_entry(form, self.fecha_var, r)

        # Producto
        r = 1
        tk.Label(form, text="Producto:", font=F_NORMAL_B,
                 bg=C["card"], fg=C["text_sec"]).grid(
                     row=r, column=0, sticky="w", pady=10)
        self.producto_var = tk.StringVar()
        productos = obtener_productos()
        combo = ttk.Combobox(form, textvariable=self.producto_var,
                              values=productos, state="readonly",
                              font=F_GRANDE, width=25)
        if productos:
            combo.current(0)
        combo.grid(row=r, column=1, sticky="ew", pady=10)
        combo.bind("<<ComboboxSelected>>",
                    lambda _: self._auto_vendido())

        # Cantidad horneada
        r = 2
        tk.Label(form, text="Horneados:", font=F_NORMAL_B,
                 bg=C["card"], fg=C["text_sec"]).grid(
                     row=r, column=0, sticky="w", pady=10)
        self.producido_var = tk.StringVar()
        _styled_entry(form, self.producido_var, r)

        # Vendidos
        r = 3
        tk.Label(form, text="Vendidos:", font=F_NORMAL_B,
                 bg=C["card"], fg=C["text_sec"]).grid(
                     row=r, column=0, sticky="w", pady=10)
        self.vendido_var = tk.StringVar()
        _styled_entry(form, self.vendido_var, r)

        self.lbl_auto = tk.Label(form, text="", font=F_SMALL,
                                  bg=C["card"], fg=C["success"])
        self.lbl_auto.grid(row=r, column=2, padx=8)

        # Sobrante
        r = 4
        tk.Label(form, text="Sobrante:", font=F_NORMAL_B,
                 bg=C["card"], fg=C["text_sec"]).grid(
                     row=r, column=0, sticky="w", pady=10)
        self.lbl_sobrante = tk.Label(form, text="--",
                                      font=F_GRANDE_B, bg=C["card"],
                                      fg=C["primary"])
        self.lbl_sobrante.grid(row=r, column=1, sticky="w", pady=10)

        # Notas
        r = 5
        tk.Label(form, text="Notas:", font=F_NORMAL_B,
                 bg=C["card"], fg=C["text_sec"]).grid(
                     row=r, column=0, sticky="w", pady=10)
        self.obs_var = tk.StringVar()
        _styled_entry(form, self.obs_var, r)

        # Calcular sobrante en tiempo real
        self.producido_var.trace_add("write", self._calc_sobrante)
        self.vendido_var.trace_add("write", self._calc_sobrante)

        # Boton guardar
        tk.Button(form, text="\u2714  Guardar Registro", font=F_BOTON,
                  bg=C["accent"], fg="white", relief="flat",
                  activebackground=C["accent_dark"], cursor="hand2",
                  padx=30, pady=10,
                  command=self._guardar).grid(
                      row=6, column=0, columnspan=2, pady=25)

        self._auto_vendido()

    def _auto_vendido(self):
        fecha = self.fecha_var.get().strip()
        producto = self.producto_var.get()
        if fecha and producto:
            try:
                vendido = obtener_vendido_dia_producto(fecha, producto)
                if vendido > 0:
                    self.vendido_var.set(str(vendido))
                    self.lbl_auto.configure(
                        text="(dato del cajero)")
                else:
                    self.lbl_auto.configure(text="")
            except Exception:
                self.lbl_auto.configure(text="")

    def _calc_sobrante(self, *_):
        try:
            p = int(self.producido_var.get())
            v = int(self.vendido_var.get())
            s = p - v
            color = C["success"] if s >= 0 else C["danger"]
            self.lbl_sobrante.configure(text=f"{s} panes", fg=color)
        except ValueError:
            self.lbl_sobrante.configure(text="--", fg=C["primary"])

    def _guardar(self):
        try:
            fecha = self.fecha_var.get().strip()
            producto = self.producto_var.get()
            producido = int(self.producido_var.get())
            vendido = int(self.vendido_var.get())
            obs = self.obs_var.get().strip()
        except ValueError:
            messagebox.showerror("Error",
                                 "Escribe numeros validos en horneados y vendidos.")
            return

        if not producto:
            messagebox.showwarning("Atencion", "Selecciona un producto.")
            return
        if producido < 0 or vendido < 0:
            messagebox.showwarning("Atencion",
                                    "Los valores no pueden ser negativos.")
            return
        if vendido > producido:
            if not messagebox.askyesno(
                    "Atencion",
                    "Vendidos es mayor que horneados.\n"
                    "Quieres guardar de todas formas?"):
                return

        try:
            datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Error",
                                 "Formato de fecha invalido. Usa AAAA-MM-DD.")
            return

        ok = guardar_registro(fecha, producto, producido, vendido, obs)
        if ok:
            messagebox.showinfo(
                "Guardado",
                f"{icono(producto)} {producto} - {fecha}\n\n"
                f"Horneados: {producido}\n"
                f"Vendidos: {vendido}\n"
                f"Sobrante: {producido - vendido}")
            self.producido_var.set("")
            self.vendido_var.set("")
            self.obs_var.set("")
        else:
            messagebox.showerror("Error", "No se pudo guardar el registro.")


# ══════════════════════════════════════════════
# HISTORIAL
# ══════════════════════════════════════════════

class VistaHistorial(tk.Frame):

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, bg=C["bg"])
        self.app = app
        self._construir()

    def _construir(self):
        hdr = tk.Frame(self, bg=C["bg"])
        hdr.pack(fill="x", padx=25, pady=(18, 5))
        tk.Label(hdr, text="\U0001F4C5  Historial", font=F_TITULO,
                 bg=C["bg"], fg=C["text"]).pack(side="left")

        # Filtros
        filtros = tk.Frame(self, bg=C["bg"])
        filtros.pack(fill="x", padx=25, pady=(5, 10))

        tk.Label(filtros, text="Producto:", font=F_NORMAL,
                 bg=C["bg"], fg=C["text_sec"]).pack(side="left")
        self.filtro_producto = tk.StringVar(value="Todos")
        combo = ttk.Combobox(
            filtros, textvariable=self.filtro_producto,
            values=["Todos"] + obtener_productos(),
            state="readonly", font=F_NORMAL, width=18)
        combo.pack(side="left", padx=8)
        combo.bind("<<ComboboxSelected>>", lambda _: self._cargar())

        tk.Label(filtros, text="Periodo:", font=F_NORMAL,
                 bg=C["bg"], fg=C["text_sec"]).pack(side="left", padx=(15, 5))

        self.dias_var = tk.IntVar(value=30)
        for d in (7, 14, 30, 60):
            b = tk.Radiobutton(
                filtros, text=f"{d}d", variable=self.dias_var,
                value=d, command=self._cargar,
                bg=C["bg"], fg=C["text"], font=F_NORMAL,
                selectcolor=C["selected"],
                activebackground=C["bg"])
            b.pack(side="left", padx=4)

        # Tabla
        tframe = tk.Frame(self, bg=C["bg"])
        tframe.pack(fill="both", expand=True, padx=25, pady=(0, 15))

        cols = ("fecha", "dia", "producto", "producido",
                "vendido", "sobrante")
        self.tabla = ttk.Treeview(tframe, columns=cols,
                                   show="headings", height=18)
        for col, titulo, ancho in [
            ("fecha", "Fecha", 105),
            ("dia", "Dia", 95),
            ("producto", "Producto", 160),
            ("producido", "Horneados", 100),
            ("vendido", "Vendidos", 100),
            ("sobrante", "Sobrante", 100),
        ]:
            self.tabla.heading(col, text=titulo)
            self.tabla.column(col, width=ancho, anchor="center")

        sb = ttk.Scrollbar(tframe, orient="vertical",
                            command=self.tabla.yview)
        self.tabla.configure(yscrollcommand=sb.set)
        self.tabla.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        self._cargar()

    def _cargar(self):
        for row in self.tabla.get_children():
            self.tabla.delete(row)

        prod = self.filtro_producto.get()
        dias = self.dias_var.get()

        try:
            registros = obtener_registros(
                prod if prod != "Todos" else None, dias=dias)
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo cargar: {e}")
            return

        for r in registros:
            sobrante = r["sobrante"]
            tag = "normal"
            if sobrante < 0:
                tag = "negativo"
            elif r["producido"] > 0 and sobrante / r["producido"] > 0.15:
                tag = "alto"

            self.tabla.insert("", "end", values=(
                r["fecha"], r["dia_semana"],
                f"{icono(r['producto'])}  {r['producto']}",
                r["producido"], r["vendido"], sobrante
            ), tags=(tag,))

        self.tabla.tag_configure("alto",
                                  background=C["warning_bg"],
                                  foreground=C["warning"])
        self.tabla.tag_configure("negativo",
                                  background=C["danger_bg"],
                                  foreground=C["danger"])


# ══════════════════════════════════════════════
# CONFIGURACION
# ══════════════════════════════════════════════

class VistaConfiguracion(tk.Frame):

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, bg=C["bg"])
        self.app = app
        self._construir()

    def _construir(self):
        canvas_s = tk.Canvas(self, bg=C["bg"], highlightthickness=0)
        sb = ttk.Scrollbar(self, orient="vertical", command=canvas_s.yview)
        scroll = tk.Frame(canvas_s, bg=C["bg"])

        scroll.bind("<Configure>",
                     lambda e: canvas_s.configure(
                         scrollregion=canvas_s.bbox("all")))
        canvas_s.create_window((0, 0), window=scroll, anchor="nw")
        canvas_s.configure(yscrollcommand=sb.set)
        canvas_s.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")
        _bind_scroll(canvas_s)

        hdr = tk.Frame(scroll, bg=C["bg"])
        hdr.pack(fill="x", padx=25, pady=(18, 5))
        tk.Label(hdr, text="\u2699\uFE0F  Configuracion",
                 font=F_TITULO, bg=C["bg"], fg=C["text"]).pack(side="left")

        # ── Productos y Precios ──
        self._seccion_productos(scroll)
        # ── Usuarios ──
        self._seccion_usuarios(scroll)
        # ── Info ──
        self._seccion_info(scroll)

    def _seccion_productos(self, parent):
        card = _card(parent, "\U0001F35E  Productos y Precios")
        card.pack(fill="x", padx=25, pady=8)

        self.prod_list = tk.Frame(card, bg=C["card"])
        self.prod_list.pack(fill="x")
        self._cargar_productos()

        # Agregar nuevo
        tk.Frame(card, bg=C["divider"], height=1).pack(
            fill="x", pady=10)
        add = tk.Frame(card, bg=C["card"])
        add.pack(fill="x")

        tk.Label(add, text="Nuevo:", font=F_NORMAL,
                 bg=C["card"], fg=C["text_sec"]).pack(side="left")
        self.new_name = tk.StringVar()
        tk.Entry(add, textvariable=self.new_name, font=F_NORMAL,
                 bg=C["bg"], fg=C["text"], relief="flat", bd=0,
                 highlightthickness=1, highlightbackground=C["border"],
                 width=16).pack(side="left", padx=6, ipady=4)

        tk.Label(add, text="$", font=F_NORMAL,
                 bg=C["card"], fg=C["text_sec"]).pack(side="left")
        self.new_price = tk.StringVar(value="10.0")
        tk.Entry(add, textvariable=self.new_price, font=F_NORMAL,
                 bg=C["bg"], fg=C["text"], relief="flat", bd=0,
                 highlightthickness=1, highlightbackground=C["border"],
                 width=7).pack(side="left", padx=4, ipady=4)

        tk.Button(add, text="+ Agregar", font=F_NORMAL_B,
                  bg=C["accent"], fg="white", relief="flat",
                  activebackground=C["accent_dark"], cursor="hand2",
                  padx=12, pady=4,
                  command=self._agregar_producto).pack(side="left", padx=8)

    def _cargar_productos(self):
        for w in self.prod_list.winfo_children():
            w.destroy()

        productos = obtener_productos_con_precio()
        for p in productos:
            row = tk.Frame(self.prod_list, bg=C["card"], pady=6)
            row.pack(fill="x")

            col_p = color_producto(p["nombre"])
            dot = tk.Canvas(row, width=14, height=14,
                            bg=C["card"], highlightthickness=0)
            dot.create_oval(2, 2, 12, 12, fill=col_p, outline=col_p)
            dot.pack(side="left", padx=(0, 8))

            tk.Label(row,
                     text=f"{icono(p['nombre'])}  {p['nombre']}",
                     font=F_GRANDE_B, bg=C["card"], fg=C["text"],
                     width=18, anchor="w").pack(side="left")

            tk.Label(row, text=f"${p['precio']:.2f}",
                     font=F_GRANDE_B, bg=C["card"],
                     fg=C["success"], width=8).pack(side="left")

            pv = tk.StringVar(value=str(p["precio"]))
            tk.Entry(row, textvariable=pv, font=F_NORMAL,
                     bg=C["bg"], fg=C["text"], relief="flat",
                     bd=0, highlightthickness=1,
                     highlightbackground=C["border"],
                     width=7).pack(side="left", padx=4, ipady=3)

            tk.Button(row, text="Cambiar", font=F_SMALL_B,
                      bg=C["primary"], fg="white", relief="flat",
                      activebackground=C["primary_dark"],
                      cursor="hand2", padx=8,
                      command=lambda n=p["nombre"], v=pv:
                          self._cambiar_precio(n, v)
                      ).pack(side="left", padx=4)

            tk.Frame(self.prod_list, bg=C["divider"],
                     height=1).pack(fill="x")

    def _cambiar_precio(self, nombre, var):
        try:
            nuevo = float(var.get())
            if nuevo < 0:
                messagebox.showwarning("Atencion",
                                        "El precio no puede ser negativo.")
                return
            if actualizar_precio(nombre, nuevo):
                self._cargar_productos()
                messagebox.showinfo("Listo",
                                     f"Precio de '{nombre}' = ${nuevo:.2f}")
        except ValueError:
            messagebox.showerror("Error", "Escribe un numero valido.")

    def _agregar_producto(self):
        nombre = self.new_name.get().strip()
        if not nombre:
            messagebox.showwarning("Atencion", "Escribe el nombre.")
            return
        try:
            precio = float(self.new_price.get())
        except ValueError:
            messagebox.showerror("Error", "Precio invalido.")
            return
        if agregar_producto(nombre, precio):
            self._cargar_productos()
            self.new_name.set("")
            messagebox.showinfo("Listo", f"'{nombre}' agregado.")
        else:
            messagebox.showwarning("Atencion", "Ese producto ya existe.")

    def _seccion_usuarios(self, parent):
        card = _card(parent, "\U0001F465  Usuarios")
        card.pack(fill="x", padx=25, pady=8)

        self.user_list = tk.Frame(card, bg=C["card"])
        self.user_list.pack(fill="x")
        self._cargar_usuarios()

        tk.Frame(card, bg=C["divider"], height=1).pack(fill="x", pady=10)

        add = tk.Frame(card, bg=C["card"])
        add.pack(fill="x")

        tk.Label(add, text="Nombre:", font=F_NORMAL,
                 bg=C["card"], fg=C["text_sec"]).pack(side="left")
        self.new_user = tk.StringVar()
        tk.Entry(add, textvariable=self.new_user, font=F_NORMAL,
                 bg=C["bg"], relief="flat", bd=0, width=12,
                 highlightthickness=1,
                 highlightbackground=C["border"]).pack(
                     side="left", padx=4, ipady=4)

        tk.Label(add, text="PIN:", font=F_NORMAL,
                 bg=C["card"], fg=C["text_sec"]).pack(side="left")
        self.new_pin = tk.StringVar()
        tk.Entry(add, textvariable=self.new_pin, font=F_NORMAL,
                 bg=C["bg"], relief="flat", bd=0, width=6,
                 highlightthickness=1,
                 highlightbackground=C["border"]).pack(
                     side="left", padx=4, ipady=4)

        tk.Label(add, text="Rol:", font=F_NORMAL,
                 bg=C["card"], fg=C["text_sec"]).pack(side="left")
        self.new_rol = tk.StringVar(value="cajero")
        ttk.Combobox(add, textvariable=self.new_rol,
                      values=["cajero", "panadero"], state="readonly",
                      font=F_NORMAL, width=10).pack(side="left", padx=4)

        tk.Button(add, text="+ Agregar", font=F_NORMAL_B,
                  bg=C["accent"], fg="white", relief="flat",
                  activebackground=C["accent_dark"], cursor="hand2",
                  padx=12, pady=4,
                  command=self._agregar_usuario).pack(side="left", padx=8)

    def _cargar_usuarios(self):
        for w in self.user_list.winfo_children():
            w.destroy()

        usuarios = obtener_usuarios()
        for u in usuarios:
            row = tk.Frame(self.user_list, bg=C["card"], pady=5)
            row.pack(fill="x")

            if u["rol"] == "panadero":
                badge_bg, badge_fg = C["primary"], "white"
            else:
                badge_bg, badge_fg = C["accent"], "white"

            badge = tk.Label(row, text=f" {u['rol'].upper()} ",
                              font=F_SMALL_B, bg=badge_bg, fg=badge_fg)
            badge.pack(side="left", padx=(0, 10))

            tk.Label(row, text=u["nombre"], font=F_GRANDE,
                     bg=C["card"], fg=C["text"]).pack(side="left")

            tk.Button(row, text="Eliminar", font=F_SMALL,
                      bg=C["danger_bg"], fg=C["danger"], relief="flat",
                      cursor="hand2",
                      command=lambda uid=u["id"]:
                          self._eliminar_usuario(uid)
                      ).pack(side="right", padx=4)

            tk.Frame(self.user_list, bg=C["divider"],
                     height=1).pack(fill="x")

    def _agregar_usuario(self):
        nombre = self.new_user.get().strip()
        pin = self.new_pin.get().strip()
        rol = self.new_rol.get()

        if not nombre or not pin:
            messagebox.showwarning("Atencion", "Llena nombre y PIN.")
            return
        if agregar_usuario(nombre, pin, rol):
            self._cargar_usuarios()
            self.new_user.set("")
            self.new_pin.set("")
            messagebox.showinfo("Listo", f"'{nombre}' agregado como {rol}.")
        else:
            messagebox.showerror("Error", "No se pudo agregar.")

    def _eliminar_usuario(self, uid):
        if messagebox.askyesno("Confirmar", "Eliminar este usuario?"):
            if eliminar_usuario(uid):
                self._cargar_usuarios()

    def _seccion_info(self, parent):
        card = _card(parent, "Acerca del Sistema")
        card.pack(fill="x", padx=25, pady=8)

        info = [
            ("Version", "2.1.0"),
            ("Motor", "Python + SQLite"),
            ("Pronostico",
             "Estimacion \u2192 Promedio Semanal \u2192 Por Dia de Semana"),
        ]
        for k, v in info:
            row = tk.Frame(card, bg=C["card"], pady=3)
            row.pack(fill="x")
            tk.Label(row, text=f"{k}:", font=F_NORMAL,
                     bg=C["card"], fg=C["text_muted"],
                     width=12, anchor="w").pack(side="left")
            tk.Label(row, text=v, font=F_NORMAL_B,
                     bg=C["card"], fg=C["primary"]).pack(side="left")


# ══════════════════════════════════════════════
# WIDGETS REUTILIZABLES
# ══════════════════════════════════════════════

def _metrica_card(parent, titulo, valor, color, emoji=""):
    """Tarjeta de metrica con icono grande."""
    card = tk.Frame(parent, bg=C["card"], padx=22, pady=14,
                     highlightbackground=C["border"],
                     highlightthickness=1)
    card.pack(side="left", padx=6, fill="both", expand=True)

    top = tk.Frame(card, bg=C["card"])
    top.pack(fill="x")

    if emoji:
        tk.Label(top, text=emoji, font=F_ICONO_SM,
                 bg=C["card"]).pack(side="left")

    tk.Label(top, text=titulo, font=F_NORMAL,
             bg=C["card"], fg=C["text_muted"]).pack(side="left", padx=6)

    tk.Label(card, text=valor, font=("Segoe UI", 26, "bold"),
             bg=C["card"], fg=color).pack(anchor="w", pady=(5, 0))


def _card(parent, titulo: str) -> tk.Frame:
    """Tarjeta con titulo y borde."""
    outer = tk.Frame(parent, bg=C["card"],
                      highlightbackground=C["border"],
                      highlightthickness=1)

    hdr = tk.Frame(outer, bg=C["card"], padx=18, pady=10)
    hdr.pack(fill="x")
    tk.Label(hdr, text=titulo, font=F_SUBTIT,
             bg=C["card"], fg=C["text_sec"]).pack(anchor="w")
    tk.Frame(outer, bg=C["divider"], height=1).pack(fill="x")

    content = tk.Frame(outer, bg=C["card"], padx=18, pady=12)
    content.pack(fill="both", expand=True)

    # Devolver el content frame para que se agreguen widgets
    outer._content = content
    return outer


def _styled_entry(parent, var, row):
    """Entry con estilo moderno."""
    e = tk.Entry(parent, textvariable=var, font=F_GRANDE,
                 bg=C["bg"], fg=C["text"],
                 insertbackground=C["accent"],
                 relief="flat", bd=0, width=28,
                 highlightthickness=2,
                 highlightbackground=C["border"],
                 highlightcolor=C["accent"])
    e.grid(row=row, column=1, sticky="ew", pady=10, ipady=6)
    return e


def _bind_scroll(canvas):
    """Bind mousewheel scroll a un canvas."""
    def _on_wheel(event):
        canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
    canvas.bind_all("<MouseWheel>", _on_wheel)
    canvas.bind_all("<Button-4>",
                     lambda e: canvas.yview_scroll(-1, "units"))
    canvas.bind_all("<Button-5>",
                     lambda e: canvas.yview_scroll(1, "units"))


# ──────────────────────────────────────────────
# Punto de entrada
# ──────────────────────────────────────────────

if __name__ == "__main__":
    app = PanaderiaApp()
    app.mainloop()
