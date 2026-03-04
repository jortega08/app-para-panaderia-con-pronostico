"""
app.py
------
Interfaz grafica de Panaderia - Sistema de Pronostico y Punto de Venta.
Diseno premium con paleta de colores moderna, graficas y dashboard visual.
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
from typing import Optional
import sys
import os
import io
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.database import (
    inicializar_base_de_datos,
    guardar_registro,
    obtener_registros,
    obtener_productos,
    obtener_productos_con_precio,
    obtener_precio,
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

# Matplotlib para graficas embebidas
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import numpy as np

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Paleta de colores premium - Tonos dorados y cafe
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
C = {
    "fondo":          "#FAF6F1",
    "fondo_nav":      "#2C1A0E",
    "fondo_nav_txt":  "#E8D5C0",
    "tarjeta":        "#FFFFFF",
    "primario":       "#C8782A",
    "primario_hover": "#A85E18",
    "primario_light": "#FFF3E6",
    "secundario":     "#5D4037",
    "dorado":         "#D4A24E",
    "dorado_light":   "#FDF6E3",
    "texto":          "#1A0F08",
    "texto_suave":    "#6D5D50",
    "verde":          "#1B7A3D",
    "verde_claro":    "#E6F7ED",
    "amarillo":       "#D4920A",
    "amarillo_claro": "#FFF9E6",
    "rojo":           "#C0392B",
    "rojo_claro":     "#FDEDEC",
    "borde":          "#E0D5C8",
    "seleccion":      "#FDE8CD",
    "sombra":         "#D6CCC2",
    "accent_bar":     "#C8782A",
}

# Fuentes modernas
FONT_FAMILY = "Segoe UI"
F_TITULO   = (FONT_FAMILY, 22, "bold")
F_SUBTIT   = (FONT_FAMILY, 17, "bold")
F_GRANDE   = (FONT_FAMILY, 15)
F_GRANDE_B = (FONT_FAMILY, 15, "bold")
F_NORMAL   = (FONT_FAMILY, 13)
F_NORMAL_B = (FONT_FAMILY, 13, "bold")
F_BOTON    = (FONT_FAMILY, 14, "bold")
F_NUMERO   = (FONT_FAMILY, 34, "bold")
F_PEQUENA  = (FONT_FAMILY, 11)
F_EMOJI    = (FONT_FAMILY, 36)

# Emojis para productos
EMOJI_PRODUCTO = {
    "Pan Frances": "\U0001F956",
    "Pan Dulce": "\U0001F369",
    "Croissant": "\U0001F950",
    "Integral": "\U0001F35E",
}
EMOJI_DEFAULT = "\U0001F35E"

# Colores para graficas matplotlib
CHART_COLORS = ["#C8782A", "#D4A24E", "#5D4037", "#1B7A3D", "#C0392B", "#3498DB", "#8E44AD"]

def _get_emoji(nombre):
    for key, emoji in EMOJI_PRODUCTO.items():
        if key.lower() in nombre.lower():
            return emoji
    return EMOJI_DEFAULT


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# APLICACION PRINCIPAL
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class PanaderiaApp(tk.Tk):
    """Ventana principal."""

    def __init__(self):
        super().__init__()
        self.title("Panaderia - Sistema de Ventas y Pronostico")
        self.geometry("1200x800")
        self.minsize(1000, 700)
        self.configure(bg=C["fondo"])

        try:
            inicializar_base_de_datos()
        except Exception as e:
            messagebox.showerror("Error de Base de Datos",
                f"No se pudo inicializar la base de datos.\n{e}")

        self.usuario_actual = None
        self._configurar_estilos()
        self._mostrar_login()

    def _configurar_estilos(self):
        s = ttk.Style(self)
        s.theme_use("clam")

        s.configure("TFrame", background=C["fondo"])
        s.configure("Nav.TFrame", background=C["fondo_nav"])
        s.configure("Card.TFrame", background=C["tarjeta"])

        s.configure("TLabel", background=C["fondo"], foreground=C["texto"],
                     font=F_NORMAL)

        s.configure("Primario.TButton",
                     background=C["primario"], foreground="white",
                     font=F_BOTON, borderwidth=0, padding=(20, 12))
        s.map("Primario.TButton",
              background=[("active", C["primario_hover"])])

        s.configure("Secundario.TButton",
                     background=C["secundario"], foreground="white",
                     font=F_NORMAL_B, borderwidth=0, padding=(16, 10))
        s.map("Secundario.TButton",
              background=[("active", "#3E2723")])

        s.configure("Nav.TButton",
                     background=C["fondo_nav"], foreground=C["fondo_nav_txt"],
                     font=F_GRANDE, borderwidth=0, padding=(16, 14))
        s.map("Nav.TButton",
              background=[("active", "#4E342E")],
              foreground=[("active", C["dorado"])])

        s.configure("NavActivo.TButton",
                     background="#4E342E", foreground=C["dorado"],
                     font=F_GRANDE_B, borderwidth=0, padding=(16, 14))

        s.configure("Verde.TButton",
                     background=C["verde"], foreground="white",
                     font=F_BOTON, borderwidth=0, padding=(20, 14))
        s.map("Verde.TButton",
              background=[("active", "#145A2E")])

        s.configure("Rojo.TButton",
                     background=C["rojo"], foreground="white",
                     font=F_NORMAL_B, borderwidth=0, padding=(14, 8))
        s.map("Rojo.TButton",
              background=[("active", "#922B21")])

        s.configure("Producto.TButton",
                     background=C["tarjeta"], foreground=C["texto"],
                     font=F_GRANDE_B, borderwidth=2, padding=(10, 20),
                     relief="solid")
        s.map("Producto.TButton",
              background=[("active", C["seleccion"])])

        s.configure("Treeview", background=C["tarjeta"],
                     foreground=C["texto"], fieldbackground=C["tarjeta"],
                     font=F_NORMAL, rowheight=38)
        s.configure("Treeview.Heading",
                     background=C["secundario"], foreground="white",
                     font=F_NORMAL_B)
        s.map("Treeview",
              background=[("selected", C["seleccion"])],
              foreground=[("selected", C["texto"])])

        s.configure("TCombobox", fieldbackground=C["tarjeta"],
                     background=C["tarjeta"], foreground=C["texto"],
                     font=F_NORMAL)

        self.option_add("*TCombobox*Listbox.font", F_NORMAL)
        self.option_add("*TCombobox*Listbox.background", C["tarjeta"])
        self.option_add("*TCombobox*Listbox.foreground", C["texto"])

    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # Login
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _mostrar_login(self):
        for w in self.winfo_children():
            w.destroy()

        self.usuario_actual = None

        frame = tk.Frame(self, bg=C["fondo"])
        frame.place(relx=0.5, rely=0.5, anchor="center")

        # Logo con emoji
        tk.Label(frame, text="\U0001F35E", font=(FONT_FAMILY, 56),
                 bg=C["fondo"]).pack(pady=(0, 5))
        tk.Label(frame, text="PANADERIA", font=(FONT_FAMILY, 38, "bold"),
                 bg=C["fondo"], fg=C["primario"]).pack(pady=(0, 2))
        tk.Label(frame, text="Sistema de Ventas y Pronostico",
                 font=F_GRANDE, bg=C["fondo"], fg=C["texto_suave"]).pack()

        # Linea decorativa dorada
        deco = tk.Frame(frame, bg=C["dorado"], height=3)
        deco.pack(fill="x", pady=25, padx=60)

        tk.Label(frame, text="Ingresa tu PIN para entrar:",
                 font=F_GRANDE, bg=C["fondo"], fg=C["texto"]).pack(pady=(0, 15))

        pin_frame = tk.Frame(frame, bg=C["fondo"])
        pin_frame.pack(pady=10)

        self.pin_var = tk.StringVar()
        self.pin_entry = tk.Entry(
            pin_frame, textvariable=self.pin_var,
            font=(FONT_FAMILY, 30, "bold"), width=8,
            justify="center", show="\u2022",
            bg=C["tarjeta"], fg=C["texto"],
            insertbackground=C["primario"],
            relief="solid", bd=2,
            highlightthickness=2,
            highlightcolor=C["dorado"],
            highlightbackground=C["borde"]
        )
        self.pin_entry.pack(pady=5)
        self.pin_entry.focus_set()
        self.pin_entry.bind("<Return>", lambda _: self._intentar_login())

        # Teclado numerico
        teclado = tk.Frame(frame, bg=C["fondo"])
        teclado.pack(pady=15)

        numeros = [
            ["1", "2", "3"],
            ["4", "5", "6"],
            ["7", "8", "9"],
            ["Borrar", "0", "Entrar"],
        ]
        for fila_nums in numeros:
            fila = tk.Frame(teclado, bg=C["fondo"])
            fila.pack()
            for num in fila_nums:
                if num == "Entrar":
                    btn = tk.Button(
                        fila, text="\u2713 Entrar", font=F_BOTON, width=7, height=2,
                        bg=C["verde"], fg="white", relief="flat",
                        activebackground="#145A2E", cursor="hand2",
                        command=self._intentar_login
                    )
                elif num == "Borrar":
                    btn = tk.Button(
                        fila, text="\u232B", font=F_BOTON, width=7, height=2,
                        bg=C["rojo"], fg="white", relief="flat",
                        activebackground="#922B21", cursor="hand2",
                        command=lambda: self.pin_var.set(self.pin_var.get()[:-1])
                    )
                else:
                    btn = tk.Button(
                        fila, text=num, font=(FONT_FAMILY, 18, "bold"),
                        width=7, height=2,
                        bg=C["tarjeta"], fg=C["texto"], relief="solid",
                        bd=1, activebackground=C["seleccion"], cursor="hand2",
                        command=lambda n=num: self.pin_var.set(
                            self.pin_var.get() + n)
                    )
                btn.pack(side="left", padx=3, pady=3)

        self.login_msg = tk.Label(frame, text="", font=F_NORMAL,
                                   bg=C["fondo"], fg=C["rojo"])
        self.login_msg.pack(pady=10)

        tk.Label(frame, text="PIN Panadero: 1234  |  PIN Cajero: 0000",
                 font=F_PEQUENA, bg=C["fondo"],
                 fg=C["texto_suave"]).pack(pady=(10, 0))

    def _intentar_login(self):
        pin = self.pin_var.get().strip()
        if not pin:
            self.login_msg.configure(text="Escribe tu PIN")
            return
        try:
            usuario = verificar_pin(pin)
        except Exception:
            self.login_msg.configure(text="Error al verificar. Intenta de nuevo.")
            return

        if usuario:
            self.usuario_actual = usuario
            self._iniciar_sesion()
        else:
            self.login_msg.configure(text="PIN incorrecto. Intenta de nuevo.")
            self.pin_var.set("")

    def _iniciar_sesion(self):
        for w in self.winfo_children():
            w.destroy()

        if self.usuario_actual["rol"] == "cajero":
            self._construir_cajero()
        else:
            self._construir_panadero()

    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # INTERFAZ CAJERO
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _construir_cajero(self):
        top = tk.Frame(self, bg=C["primario"], height=56)
        top.pack(fill="x")
        top.pack_propagate(False)

        tk.Label(top, text=f"\U0001F9D1\u200D\U0001F4BC  Cajero: {self.usuario_actual['nombre']}",
                 font=F_GRANDE_B, bg=C["primario"], fg="white"
                 ).pack(side="left", padx=20)

        tk.Button(top, text="Cerrar Sesion", font=F_NORMAL_B,
                  bg=C["primario_hover"], fg="white", relief="flat",
                  activebackground=C["rojo"], cursor="hand2",
                  command=self._mostrar_login
                  ).pack(side="right", padx=20, pady=10)

        self.cajero_nav = tk.Frame(self, bg=C["secundario"])
        self.cajero_nav.pack(fill="x")

        self.cajero_content = tk.Frame(self, bg=C["fondo"])
        self.cajero_content.pack(fill="both", expand=True)

        self._cajero_tabs = {}
        for texto, vista in [("\U0001F6D2 Registrar Venta", "pos"),
                              ("\U0001F4CA Ventas de Hoy", "resumen")]:
            btn = ttk.Button(self.cajero_nav, text=texto, style="Nav.TButton",
                             command=lambda v=vista: self._cajero_vista(v))
            btn.pack(side="left", padx=2, pady=5)
            self._cajero_tabs[vista] = btn

        self._cajero_vista("pos")

    def _cajero_vista(self, nombre):
        for w in self.cajero_content.winfo_children():
            w.destroy()
        for key, btn in self._cajero_tabs.items():
            btn.configure(style="NavActivo.TButton" if key == nombre
                         else "Nav.TButton")

        try:
            if nombre == "pos":
                VistaPOS(self.cajero_content, self).pack(fill="both", expand=True)
            elif nombre == "resumen":
                VistaResumenDia(self.cajero_content, self).pack(
                    fill="both", expand=True)
        except Exception as e:
            _mostrar_error_vista(self.cajero_content, e)

    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # INTERFAZ PANADERO
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _construir_panadero(self):
        top = tk.Frame(self, bg=C["secundario"], height=56)
        top.pack(fill="x")
        top.pack_propagate(False)

        tk.Label(top, text=f"\U0001F468\u200D\U0001F373  Panadero: {self.usuario_actual['nombre']}",
                 font=F_GRANDE_B, bg=C["secundario"], fg="white"
                 ).pack(side="left", padx=20)

        tk.Button(top, text="Cerrar Sesion", font=F_NORMAL_B,
                  bg="#3E2723", fg="white", relief="flat",
                  activebackground=C["rojo"], cursor="hand2",
                  command=self._mostrar_login
                  ).pack(side="right", padx=20, pady=10)

        body = tk.Frame(self, bg=C["fondo"])
        body.pack(fill="both", expand=True)

        nav = tk.Frame(body, bg=C["fondo_nav"], width=230)
        nav.pack(side="left", fill="y")
        nav.pack_propagate(False)

        # Logo pequeno en nav
        tk.Label(nav, text="\U0001F35E", font=(FONT_FAMILY, 28),
                 bg=C["fondo_nav"]).pack(pady=(18, 2))
        tk.Label(nav, text="Panaderia", font=(FONT_FAMILY, 14, "bold"),
                 bg=C["fondo_nav"], fg=C["dorado"]).pack(pady=(0, 10))

        sep = tk.Frame(nav, bg=C["dorado"], height=1)
        sep.pack(fill="x", padx=20, pady=5)

        self.panadero_content = tk.Frame(body, bg=C["fondo"])
        self.panadero_content.pack(side="left", fill="both", expand=True)

        self._panadero_tabs = {}
        vistas = [
            ("\U0001F4CA Cuantos Hornear", "pronostico"),
            ("\U0001F35E Registrar Produccion", "produccion"),
            ("\U0001F4B0 Ventas de Hoy", "ventas"),
            ("\U0001F4CB Historial", "historial"),
            ("\u2699\uFE0F Configuracion", "config"),
        ]
        for texto, vista in vistas:
            btn = ttk.Button(nav, text=texto, style="Nav.TButton",
                             command=lambda v=vista: self._panadero_vista(v))
            btn.pack(fill="x", padx=8, pady=3)
            self._panadero_tabs[vista] = btn

        # Fecha actual
        tk.Label(nav, text=datetime.now().strftime("%d/%m/%Y"),
                 font=F_GRANDE, bg=C["fondo_nav"],
                 fg=C["fondo_nav_txt"]).pack(side="bottom", pady=20)

        self._panadero_vista("pronostico")

    def _panadero_vista(self, nombre):
        for w in self.panadero_content.winfo_children():
            w.destroy()
        for key, btn in self._panadero_tabs.items():
            btn.configure(style="NavActivo.TButton" if key == nombre
                         else "Nav.TButton")

        vistas = {
            "pronostico": VistaPronostico,
            "produccion": VistaProduccion,
            "ventas":     VistaVentasPanadero,
            "historial":  VistaHistorial,
            "config":     VistaConfiguracion,
        }
        try:
            if nombre in vistas:
                vistas[nombre](self.panadero_content, self).pack(
                    fill="both", expand=True)
        except Exception as e:
            _mostrar_error_vista(self.panadero_content, e)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# VISTAS DEL CAJERO
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class VistaPOS(ttk.Frame):
    """Punto de venta con botones grandes y emojis."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        header = tk.Frame(self, bg=C["fondo"])
        header.pack(fill="x", padx=25, pady=(20, 10))

        tk.Label(header, text="\U0001F6D2 Registrar Venta",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]).pack(side="left")

        main = tk.Frame(self, bg=C["fondo"])
        main.pack(fill="both", expand=True, padx=25, pady=10)

        # --- Panel de productos ---
        prod_frame = tk.Frame(main, bg=C["fondo"])
        prod_frame.pack(side="left", fill="both", expand=True, padx=(0, 15))

        tk.Label(prod_frame, text="Selecciona el producto:",
                 font=F_GRANDE, bg=C["fondo"],
                 fg=C["texto_suave"]).pack(anchor="w", pady=(0, 10))

        try:
            self.productos = obtener_productos_con_precio()
        except Exception:
            self.productos = []

        self.producto_seleccionado = None

        grid = tk.Frame(prod_frame, bg=C["fondo"])
        grid.pack(fill="both", expand=True)

        self._botones_producto = []
        for i, p in enumerate(self.productos):
            emoji = _get_emoji(p["nombre"])
            btn_frame = tk.Frame(grid, bg=C["sombra"], padx=2, pady=2)
            fila = i // 2
            col = i % 2
            btn_frame.grid(row=fila, column=col, padx=8, pady=8, sticky="nsew")
            grid.grid_columnconfigure(col, weight=1)
            grid.grid_rowconfigure(fila, weight=1)

            inner = tk.Frame(btn_frame, bg=C["tarjeta"])
            inner.pack(fill="both", expand=True)

            btn = tk.Button(
                inner,
                text=f"{emoji}\n{p['nombre']}\n${p['precio']:.2f}",
                font=F_GRANDE_B, bg=C["tarjeta"], fg=C["texto"],
                relief="flat", activebackground=C["seleccion"],
                cursor="hand2", pady=12,
                command=lambda prod=p: self._seleccionar_producto(prod)
            )
            btn.pack(fill="both", expand=True, padx=3, pady=3)
            self._botones_producto.append((btn, p))

        # --- Panel de venta ---
        venta_outer = tk.Frame(main, bg=C["sombra"], padx=2, pady=2)
        venta_outer.pack(side="right", fill="y")

        venta_frame = tk.Frame(venta_outer, bg=C["tarjeta"],
                                padx=25, pady=20, width=320)
        venta_frame.pack(fill="both", expand=True)
        venta_frame.pack_propagate(False)

        # Accent bar
        tk.Frame(venta_frame, bg=C["dorado"], height=4).pack(fill="x", pady=(0, 15))

        tk.Label(venta_frame, text="\U0001F4DD Detalle de Venta",
                 font=F_SUBTIT, bg=C["tarjeta"],
                 fg=C["secundario"]).pack(pady=(0, 15))

        tk.Label(venta_frame, text="Producto:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(anchor="w")
        self.lbl_producto = tk.Label(venta_frame, text="(ninguno)",
                                      font=F_GRANDE_B, bg=C["tarjeta"],
                                      fg=C["primario"])
        self.lbl_producto.pack(anchor="w", pady=(0, 15))

        tk.Label(venta_frame, text="Cantidad:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(anchor="w")

        cant_frame = tk.Frame(venta_frame, bg=C["tarjeta"])
        cant_frame.pack(fill="x", pady=(5, 15))

        self.cantidad_var = tk.IntVar(value=1)

        tk.Button(cant_frame, text=" \u2212 ", font=(FONT_FAMILY, 20, "bold"),
                  bg=C["rojo_claro"], fg=C["rojo"], relief="flat", width=3,
                  cursor="hand2",
                  command=self._decrementar).pack(side="left")

        self.lbl_cantidad = tk.Label(cant_frame,
                                      textvariable=self.cantidad_var,
                                      font=F_NUMERO, bg=C["tarjeta"],
                                      fg=C["texto"], width=4)
        self.lbl_cantidad.pack(side="left", expand=True)

        tk.Button(cant_frame, text=" + ", font=(FONT_FAMILY, 20, "bold"),
                  bg=C["verde_claro"], fg=C["verde"], relief="flat", width=3,
                  cursor="hand2",
                  command=self._incrementar).pack(side="left")

        tk.Frame(venta_frame, bg=C["borde"], height=2).pack(fill="x", pady=10)

        tk.Label(venta_frame, text="Total:", font=F_GRANDE,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(anchor="w")
        self.lbl_total = tk.Label(venta_frame, text="$0.00",
                                   font=(FONT_FAMILY, 30, "bold"),
                                   bg=C["tarjeta"], fg=C["verde"])
        self.lbl_total.pack(anchor="w", pady=(5, 20))

        self.btn_registrar = ttk.Button(
            venta_frame, text="\u2713 Registrar Venta",
            style="Verde.TButton",
            command=self._registrar_venta
        )
        self.btn_registrar.pack(fill="x", pady=(10, 0))

        self._actualizar_total()

    def _seleccionar_producto(self, producto):
        self.producto_seleccionado = producto
        emoji = _get_emoji(producto["nombre"])
        self.lbl_producto.configure(text=f"{emoji} {producto['nombre']}")
        for btn, p in self._botones_producto:
            if p["nombre"] == producto["nombre"]:
                btn.configure(bg=C["seleccion"], relief="flat")
            else:
                btn.configure(bg=C["tarjeta"], relief="flat")
        self._actualizar_total()

    def _incrementar(self):
        self.cantidad_var.set(self.cantidad_var.get() + 1)
        self._actualizar_total()

    def _decrementar(self):
        val = self.cantidad_var.get()
        if val > 1:
            self.cantidad_var.set(val - 1)
            self._actualizar_total()

    def _actualizar_total(self):
        if self.producto_seleccionado:
            total = self.producto_seleccionado["precio"] * self.cantidad_var.get()
            self.lbl_total.configure(text=f"${total:.2f}")
        else:
            self.lbl_total.configure(text="$0.00")

    def _registrar_venta(self):
        if not self.producto_seleccionado:
            messagebox.showwarning("Atencion", "Selecciona un producto primero.")
            return

        producto = self.producto_seleccionado["nombre"]
        cantidad = self.cantidad_var.get()
        precio = self.producto_seleccionado["precio"]
        usuario = self.app.usuario_actual["nombre"]

        try:
            exito = registrar_venta(producto, cantidad, precio, usuario)
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo registrar la venta.\n{e}")
            return

        if exito:
            total = precio * cantidad
            messagebox.showinfo(
                "Venta Registrada",
                f"{cantidad}x {producto}\n"
                f"Total: ${total:.2f}\n\n"
                f"Registrado correctamente."
            )
            self.cantidad_var.set(1)
            self.producto_seleccionado = None
            self.lbl_producto.configure(text="(ninguno)")
            for btn, _ in self._botones_producto:
                btn.configure(bg=C["tarjeta"])
            self._actualizar_total()
        else:
            messagebox.showerror("Error", "No se pudo registrar la venta.")


class VistaResumenDia(ttk.Frame):
    """Resumen de ventas del dia con graficas."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._auto_id = None
        self._construir()

    def _construir(self):
        header = tk.Frame(self, bg=C["fondo"])
        header.pack(fill="x", padx=25, pady=(20, 10))

        tk.Label(header, text="\U0001F4CA Ventas de Hoy",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]).pack(side="left")

        ttk.Button(header, text="\u21BB Actualizar", style="Primario.TButton",
                   command=self._actualizar).pack(side="right")

        # Status label
        self.lbl_status = tk.Label(header, text="", font=F_PEQUENA,
                                    bg=C["fondo"], fg=C["verde"])
        self.lbl_status.pack(side="right", padx=15)

        self.resumen_frame = tk.Frame(self, bg=C["fondo"])
        self.resumen_frame.pack(fill="x", padx=25, pady=10)

        self.chart_frame = tk.Frame(self, bg=C["fondo"])
        self.chart_frame.pack(fill="x", padx=25, pady=(0, 5))

        self.tabla_frame = tk.Frame(self, bg=C["fondo"])
        self.tabla_frame.pack(fill="both", expand=True, padx=25, pady=10)

        self._actualizar()
        self._auto_refresh()

    def _auto_refresh(self):
        """Auto actualizar cada 30 segundos."""
        try:
            if self.winfo_exists():
                self._actualizar(silent=True)
                self._auto_id = self.after(30000, self._auto_refresh)
        except Exception:
            pass

    def destroy(self):
        if self._auto_id:
            self.after_cancel(self._auto_id)
        super().destroy()

    def _actualizar(self, silent=False):
        try:
            for w in self.resumen_frame.winfo_children():
                w.destroy()
            for w in self.chart_frame.winfo_children():
                w.destroy()
            for w in self.tabla_frame.winfo_children():
                w.destroy()

            totales = obtener_total_ventas_dia()
            resumen = obtener_resumen_ventas_dia()

            # Tarjetas KPI
            cards = tk.Frame(self.resumen_frame, bg=C["fondo"])
            cards.pack(fill="x")

            _tarjeta_kpi(cards, "\U0001F4B0 Total Vendido",
                     f"${totales['dinero']:.2f}", C["verde"])
            _tarjeta_kpi(cards, "\U0001F35E Panes Vendidos",
                     str(totales["panes"]), C["primario"])
            _tarjeta_kpi(cards, "\U0001F4CB Transacciones",
                     str(totales["transacciones"]), C["dorado"])

            # Graficas
            if resumen:
                self._crear_graficas(resumen)

            # Tabla
            if resumen:
                tk.Label(self.tabla_frame, text="Detalle por Producto",
                         font=F_SUBTIT, bg=C["fondo"],
                         fg=C["secundario"]).pack(anchor="w", pady=(10, 5))

                cols = ("producto", "cantidad", "total")
                tabla = ttk.Treeview(self.tabla_frame, columns=cols,
                                      show="headings", height=8)
                tabla.heading("producto", text="Producto")
                tabla.heading("cantidad", text="Cantidad")
                tabla.heading("total", text="Total $")
                tabla.column("producto", width=200)
                tabla.column("cantidad", width=120, anchor="center")
                tabla.column("total", width=150, anchor="center")

                for r in resumen:
                    tabla.insert("", "end", values=(
                        f"{_get_emoji(r['producto'])} {r['producto']}",
                        r["total_cantidad"],
                        f"${r['total_dinero']:.2f}"
                    ))
                tabla.pack(fill="both", expand=True)
            else:
                tk.Label(self.tabla_frame,
                         text="\U0001F4ED No hay ventas registradas hoy.\n\n"
                              "Ve a 'Registrar Venta' para comenzar.",
                         font=F_GRANDE, bg=C["fondo"],
                         fg=C["texto_suave"]).pack(expand=True)

            if not silent:
                ahora = datetime.now().strftime("%H:%M:%S")
                self.lbl_status.configure(text=f"\u2713 Actualizado {ahora}")

        except Exception as e:
            tk.Label(self.tabla_frame,
                     text=f"\u26A0 Error al cargar datos.\nIntenta actualizar.\n\n{e}",
                     font=F_NORMAL, bg=C["fondo"],
                     fg=C["rojo"]).pack(expand=True, pady=30)

    def _crear_graficas(self, resumen):
        fig = Figure(figsize=(8, 2.5), dpi=90)
        fig.patch.set_facecolor(C["fondo"])

        # Grafica de barras horizontal
        ax1 = fig.add_subplot(121)
        nombres = [r["producto"] for r in resumen]
        cantidades = [r["total_cantidad"] for r in resumen]
        colores = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(nombres))]

        bars = ax1.barh(nombres, cantidades, color=colores, height=0.6, edgecolor="white")
        ax1.set_title("Cantidad Vendida", fontsize=11, fontweight="bold", color=C["texto"])
        ax1.set_facecolor(C["fondo"])
        ax1.tick_params(colors=C["texto_suave"], labelsize=9)
        ax1.spines["top"].set_visible(False)
        ax1.spines["right"].set_visible(False)
        for bar, cant in zip(bars, cantidades):
            ax1.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                    str(cant), va="center", fontsize=9, color=C["texto"])

        # Grafica de dona
        ax2 = fig.add_subplot(122)
        dineros = [r["total_dinero"] for r in resumen]
        wedges, texts, autotexts = ax2.pie(
            dineros, labels=nombres, colors=colores, autopct="%1.0f%%",
            startangle=90, pctdistance=0.75,
            textprops={"fontsize": 8, "color": C["texto"]}
        )
        centre_circle = plt.Circle((0, 0), 0.55, fc=C["fondo"])
        ax2.add_artist(centre_circle)
        ax2.set_title("Distribucion de Ventas $", fontsize=11, fontweight="bold", color=C["texto"])

        fig.tight_layout(pad=2)

        canvas = FigureCanvasTkAgg(fig, master=self.chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="x")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# VISTAS DEL PANADERO
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class VistaPronostico(ttk.Frame):
    """Dashboard de pronostico con graficas."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        canvas = tk.Canvas(self, bg=C["fondo"], highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        self.scroll_frame = tk.Frame(canvas, bg=C["fondo"])

        self.scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=self.scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        header = tk.Frame(self.scroll_frame, bg=C["fondo"])
        header.pack(fill="x", padx=25, pady=(20, 5))

        tk.Label(header, text="\U0001F4CA Cuantos Hornear Hoy",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]).pack(side="left")

        fecha_hoy = datetime.now().strftime("%d/%m/%Y")
        tk.Label(header, text=fecha_hoy, font=F_GRANDE,
                 bg=C["fondo"], fg=C["texto_suave"]).pack(side="right")

        try:
            productos = obtener_productos()
        except Exception:
            productos = []

        if not productos:
            tk.Label(self.scroll_frame,
                     text="No hay productos. Ve a Configuracion para agregar.",
                     font=F_GRANDE, bg=C["fondo"],
                     fg=C["texto_suave"]).pack(pady=50)
            return

        for producto in productos:
            try:
                self._tarjeta_pronostico(self.scroll_frame, producto)
            except Exception as e:
                _mostrar_error_mini(self.scroll_frame, producto, e)

    def _tarjeta_pronostico(self, parent, producto: str):
        resultado = calcular_pronostico(producto)
        registros = obtener_registros(producto, dias=7)
        eficiencia = calcular_eficiencia(registros)
        tendencia = analizar_tendencia(registros)

        color_estado = {
            "bien": C["verde"], "alerta": C["amarillo"], "problema": C["rojo"],
        }.get(resultado.estado, C["texto_suave"])

        color_fondo = {
            "bien": C["verde_claro"], "alerta": C["amarillo_claro"], "problema": C["rojo_claro"],
        }.get(resultado.estado, C["fondo"])

        emoji = _get_emoji(producto)

        # Tarjeta con sombra
        shadow = tk.Frame(parent, bg=C["sombra"], padx=1, pady=1)
        shadow.pack(fill="x", padx=25, pady=8)

        card = tk.Frame(shadow, bg=C["tarjeta"])
        card.pack(fill="x")

        # Accent bar izquierdo simulado con frame de color
        main_row = tk.Frame(card, bg=C["tarjeta"])
        main_row.pack(fill="x")

        accent = tk.Frame(main_row, bg=color_estado, width=5)
        accent.pack(side="left", fill="y")

        inner = tk.Frame(main_row, bg=C["tarjeta"], padx=20, pady=15)
        inner.pack(side="left", fill="both", expand=True)

        # Fila 1: Emoji + Nombre + Cantidad sugerida
        fila1 = tk.Frame(inner, bg=C["tarjeta"])
        fila1.pack(fill="x")

        tk.Label(fila1, text=f"{emoji} {producto}", font=F_SUBTIT,
                 bg=C["tarjeta"], fg=C["texto"]).pack(side="left")

        num_frame = tk.Frame(fila1, bg=color_fondo, padx=15, pady=5)
        num_frame.pack(side="right")

        tk.Label(num_frame, text=f"{resultado.produccion_sugerida}",
                 font=(FONT_FAMILY, 28, "bold"), bg=color_fondo,
                 fg=color_estado).pack(side="left")
        tk.Label(num_frame, text=" panes", font=F_GRANDE,
                 bg=color_fondo, fg=color_estado).pack(side="left")

        # Fila 2: Detalles
        fila2 = tk.Frame(inner, bg=C["tarjeta"])
        fila2.pack(fill="x", pady=(8, 0))

        detalles = [
            f"Promedio: {resultado.promedio_ventas} vendidos/dia",
            f"Tendencia: {tendencia}",
        ]
        if eficiencia:
            detalles.append(
                f"Aprovechamiento: {eficiencia.get('tasa_aprovechamiento', 0)}%"
            )
        detalles.append(f"Datos: {resultado.dias_historial} dias")

        for d in detalles:
            tk.Label(fila2, text=d, font=F_NORMAL,
                     bg=C["tarjeta"], fg=C["texto_suave"]
                     ).pack(side="left", padx=(0, 20))

        # Fila 3: Mensaje
        msg_frame = tk.Frame(inner, bg=color_fondo, padx=10, pady=5)
        msg_frame.pack(fill="x", pady=(8, 0))

        tk.Label(msg_frame, text=resultado.mensaje,
                 font=F_NORMAL, bg=color_fondo,
                 fg=color_estado).pack(anchor="w")

        # Fila 4: Grafica de ventas ultimos 7 dias
        if registros and len(registros) >= 2:
            self._mini_grafica(inner, registros, color_estado)

    def _mini_grafica(self, parent, registros, color):
        fig = Figure(figsize=(6, 1.6), dpi=85)
        fig.patch.set_facecolor(C["tarjeta"])
        ax = fig.add_subplot(111)

        fechas = [r["fecha"][-5:] for r in reversed(registros)]
        vendidos = [r["vendido"] for r in reversed(registros)]

        ax.bar(fechas, vendidos, color=color, alpha=0.7, width=0.6, edgecolor="white")
        ax.plot(fechas, vendidos, color=color, marker="o", markersize=4, linewidth=1.5)
        ax.set_title("Ventas ultimos dias", fontsize=9, color=C["texto_suave"])
        ax.set_facecolor(C["tarjeta"])
        ax.tick_params(colors=C["texto_suave"], labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(C["borde"])
        ax.spines["bottom"].set_color(C["borde"])
        fig.tight_layout(pad=1.5)

        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="x", pady=(8, 0))


class VistaProduccion(ttk.Frame):
    """Registro de produccion diaria."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        tk.Label(self, text="\U0001F35E Registrar Produccion del Dia",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"],
                 ).pack(fill="x", padx=25, pady=(20, 10))

        tk.Label(self, text="Registra cuantos panes se hornearon hoy.",
                 font=F_NORMAL, bg=C["fondo"],
                 fg=C["texto_suave"]).pack(padx=25, anchor="w")

        form_outer = tk.Frame(self, bg=C["fondo"])
        form_outer.pack(expand=True)

        shadow = tk.Frame(form_outer, bg=C["sombra"], padx=2, pady=2)
        shadow.pack(padx=20)
        form = tk.Frame(shadow, bg=C["tarjeta"], padx=35, pady=30)
        form.pack()

        # Accent bar
        tk.Frame(form, bg=C["dorado"], height=3).grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0,15))

        _campo_label(form, "Fecha:", 1)
        self.fecha_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        _campo_entry(form, self.fecha_var, 1)

        _campo_label(form, "Producto:", 2)
        self.producto_var = tk.StringVar()
        try:
            productos = obtener_productos()
        except Exception:
            productos = []
        combo = ttk.Combobox(form, textvariable=self.producto_var,
                              values=productos, state="readonly",
                              font=F_GRANDE, width=25)
        if productos:
            combo.current(0)
        combo.grid(row=2, column=1, sticky="ew", pady=10)
        combo.bind("<<ComboboxSelected>>", lambda _: self._actualizar_vendido())

        _campo_label(form, "Cantidad horneada:", 3)
        self.producido_var = tk.StringVar()
        _campo_entry(form, self.producido_var, 3)

        _campo_label(form, "Cantidad vendida:", 4)
        self.vendido_var = tk.StringVar()
        _campo_entry(form, self.vendido_var, 4)

        self.lbl_auto_vendido = tk.Label(
            form, text="", font=F_PEQUENA,
            bg=C["tarjeta"], fg=C["verde"])
        self.lbl_auto_vendido.grid(row=4, column=2, padx=10)

        _campo_label(form, "Sobrante:", 5)
        self.lbl_sobrante = tk.Label(form, text="--", font=F_GRANDE_B,
                                      bg=C["tarjeta"], fg=C["primario"])
        self.lbl_sobrante.grid(row=5, column=1, sticky="w", pady=10)

        _campo_label(form, "Notas (opcional):", 6)
        self.obs_var = tk.StringVar()
        _campo_entry(form, self.obs_var, 6)

        for var in (self.producido_var, self.vendido_var):
            var.trace_add("write", self._actualizar_sobrante)

        ttk.Button(form, text="\u2713 Guardar Registro",
                   style="Primario.TButton",
                   command=self._guardar
                   ).grid(row=7, column=0, columnspan=2, pady=25)

        self._actualizar_vendido()

    def _actualizar_vendido(self):
        fecha = self.fecha_var.get().strip()
        producto = self.producto_var.get()
        if fecha and producto:
            try:
                vendido = obtener_vendido_dia_producto(fecha, producto)
                if vendido > 0:
                    self.vendido_var.set(str(vendido))
                    self.lbl_auto_vendido.configure(text="(desde ventas del cajero)")
                else:
                    self.lbl_auto_vendido.configure(text="")
            except Exception:
                self.lbl_auto_vendido.configure(text="")

    def _actualizar_sobrante(self, *_):
        try:
            prod = int(self.producido_var.get())
            vend = int(self.vendido_var.get())
            sobrante = prod - vend
            color = C["verde"] if sobrante >= 0 else C["rojo"]
            self.lbl_sobrante.configure(text=f"{sobrante} panes", fg=color)
        except ValueError:
            self.lbl_sobrante.configure(text="--", fg=C["primario"])

    def _guardar(self):
        try:
            fecha = self.fecha_var.get().strip()
            producto = self.producto_var.get()
            producido = int(self.producido_var.get())
            vendido = int(self.vendido_var.get())
            obs = self.obs_var.get().strip()

            if not producto:
                messagebox.showwarning("Atencion", "Selecciona un producto.")
                return
            if producido < 0 or vendido < 0:
                messagebox.showwarning("Atencion", "Los valores no pueden ser negativos.")
                return
            if vendido > producido:
                messagebox.showwarning("Atencion", "Se vendio mas de lo que se horneo.\nRevisa los datos.")
                return
            datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError as e:
            messagebox.showerror("Error", f"Datos invalidos: {e}")
            return

        try:
            exito = guardar_registro(fecha, producto, producido, vendido, obs)
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo guardar.\n{e}")
            return

        if exito:
            messagebox.showinfo(
                "Guardado",
                f"Registro guardado:\n{producto} - {fecha}\n"
                f"Horneados: {producido} | Vendidos: {vendido} | "
                f"Sobrante: {producido - vendido}")
            self.producido_var.set("")
            self.vendido_var.set("")
            self.obs_var.set("")
        else:
            messagebox.showerror("Error", "No se pudo guardar.")


class VistaVentasPanadero(ttk.Frame):
    """Vista de ventas del dia para el panadero con dashboard."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._auto_id = None
        self._construir()

    def _construir(self):
        header = tk.Frame(self, bg=C["fondo"])
        header.pack(fill="x", padx=25, pady=(20, 10))

        tk.Label(header, text="\U0001F4B0 Ventas de Hoy",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]).pack(side="left")

        ttk.Button(header, text="\u21BB Actualizar", style="Primario.TButton",
                   command=self._actualizar).pack(side="right")

        self.lbl_status = tk.Label(header, text="", font=F_PEQUENA,
                                    bg=C["fondo"], fg=C["verde"])
        self.lbl_status.pack(side="right", padx=15)

        self.resumen_frame = tk.Frame(self, bg=C["fondo"])
        self.resumen_frame.pack(fill="x", padx=25)

        self.chart_frame = tk.Frame(self, bg=C["fondo"])
        self.chart_frame.pack(fill="x", padx=25, pady=(5, 0))

        self.lista_frame = tk.Frame(self, bg=C["fondo"])
        self.lista_frame.pack(fill="both", expand=True, padx=25, pady=10)

        self._actualizar()
        self._auto_refresh()

    def _auto_refresh(self):
        try:
            if self.winfo_exists():
                self._actualizar(silent=True)
                self._auto_id = self.after(30000, self._auto_refresh)
        except Exception:
            pass

    def destroy(self):
        if self._auto_id:
            self.after_cancel(self._auto_id)
        super().destroy()

    def _actualizar(self, silent=False):
        try:
            for w in self.resumen_frame.winfo_children():
                w.destroy()
            for w in self.chart_frame.winfo_children():
                w.destroy()
            for w in self.lista_frame.winfo_children():
                w.destroy()

            totales = obtener_total_ventas_dia()
            resumen = obtener_resumen_ventas_dia()
            ventas = obtener_ventas_dia()

            # Tarjetas KPI
            cards = tk.Frame(self.resumen_frame, bg=C["fondo"])
            cards.pack(fill="x", pady=(0, 10))

            _tarjeta_kpi(cards, "\U0001F4B0 Total del Dia",
                     f"${totales['dinero']:.2f}", C["verde"])
            _tarjeta_kpi(cards, "\U0001F35E Panes Vendidos",
                     str(totales["panes"]), C["primario"])
            _tarjeta_kpi(cards, "\U0001F4CB Transacciones",
                     str(totales["transacciones"]), C["dorado"])

            if not ventas:
                tk.Label(self.lista_frame,
                         text="\U0001F4ED No hay ventas registradas hoy.",
                         font=F_GRANDE, bg=C["fondo"],
                         fg=C["texto_suave"]).pack(pady=30)
                if not silent:
                    ahora = datetime.now().strftime("%H:%M:%S")
                    self.lbl_status.configure(text=f"\u2713 Actualizado {ahora}")
                return

            # Graficas
            if resumen:
                self._crear_graficas_ventas(resumen)

            # Resumen por producto
            tk.Label(self.lista_frame, text="Por Producto:",
                     font=F_SUBTIT, bg=C["fondo"],
                     fg=C["secundario"]).pack(anchor="w", pady=(5, 5))

            for r in resumen:
                emoji = _get_emoji(r["producto"])
                f = tk.Frame(self.lista_frame, bg=C["tarjeta"],
                              padx=15, pady=12, bd=0)
                f.pack(fill="x", pady=3)

                # Sombra simulada
                shadow = tk.Frame(self.lista_frame, bg=C["sombra"], height=1)
                shadow.pack(fill="x", padx=5)

                tk.Label(f, text=f"{emoji} {r['producto']}", font=F_GRANDE_B,
                         bg=C["tarjeta"], fg=C["texto"]).pack(side="left")
                tk.Label(f, text=f"${r['total_dinero']:.2f}",
                         font=F_GRANDE_B, bg=C["tarjeta"],
                         fg=C["verde"]).pack(side="right")
                tk.Label(f, text=f"{r['total_cantidad']} panes  |  ",
                         font=F_NORMAL, bg=C["tarjeta"],
                         fg=C["texto_suave"]).pack(side="right")

            # Detalle transacciones
            tk.Label(self.lista_frame, text="Ultimas Transacciones:",
                     font=F_SUBTIT, bg=C["fondo"],
                     fg=C["secundario"]).pack(anchor="w", pady=(15, 5))

            cols = ("hora", "producto", "cantidad", "total")
            tabla = ttk.Treeview(self.lista_frame, columns=cols,
                                  show="headings", height=8)
            tabla.heading("hora", text="Hora")
            tabla.heading("producto", text="Producto")
            tabla.heading("cantidad", text="Cantidad")
            tabla.heading("total", text="Total")
            tabla.column("hora", width=100, anchor="center")
            tabla.column("producto", width=200)
            tabla.column("cantidad", width=100, anchor="center")
            tabla.column("total", width=120, anchor="center")

            for v in ventas:
                tabla.insert("", "end", values=(
                    v["hora"][:5],
                    f"{_get_emoji(v['producto'])} {v['producto']}",
                    v["cantidad"],
                    f"${v['total']:.2f}"
                ))

            tabla.pack(fill="both", expand=True)

            if not silent:
                ahora = datetime.now().strftime("%H:%M:%S")
                self.lbl_status.configure(text=f"\u2713 Actualizado {ahora}")

        except Exception as e:
            tk.Label(self.lista_frame,
                     text=f"\u26A0 Error al cargar ventas.\nIntenta actualizar.\n\n{e}",
                     font=F_NORMAL, bg=C["fondo"],
                     fg=C["rojo"]).pack(expand=True, pady=30)

    def _crear_graficas_ventas(self, resumen):
        fig = Figure(figsize=(7, 2.2), dpi=85)
        fig.patch.set_facecolor(C["fondo"])

        ax1 = fig.add_subplot(121)
        nombres = [r["producto"] for r in resumen]
        cantidades = [r["total_cantidad"] for r in resumen]
        colores = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(nombres))]

        bars = ax1.bar(nombres, cantidades, color=colores, width=0.6, edgecolor="white")
        ax1.set_title("Cantidad por Producto", fontsize=10, fontweight="bold", color=C["texto"])
        ax1.set_facecolor(C["fondo"])
        ax1.tick_params(colors=C["texto_suave"], labelsize=8)
        ax1.spines["top"].set_visible(False)
        ax1.spines["right"].set_visible(False)
        for bar, cant in zip(bars, cantidades):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                    str(cant), ha="center", fontsize=9, color=C["texto"])

        ax2 = fig.add_subplot(122)
        dineros = [r["total_dinero"] for r in resumen]
        wedges, texts, autotexts = ax2.pie(
            dineros, labels=nombres, colors=colores, autopct="%1.0f%%",
            startangle=90, pctdistance=0.75,
            textprops={"fontsize": 8, "color": C["texto"]}
        )
        centre_circle = plt.Circle((0, 0), 0.55, fc=C["fondo"])
        ax2.add_artist(centre_circle)
        ax2.set_title("Distribucion $", fontsize=10, fontweight="bold", color=C["texto"])

        fig.tight_layout(pad=2)
        canvas = FigureCanvasTkAgg(fig, master=self.chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="x")


class VistaHistorial(ttk.Frame):
    """Historial de registros de produccion."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        tk.Label(self, text="\U0001F4CB Historial de Produccion",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]
                 ).pack(fill="x", padx=25, pady=(20, 10))

        filtros = tk.Frame(self, bg=C["fondo"], padx=25)
        filtros.pack(fill="x", pady=(0, 10))

        tk.Label(filtros, text="Producto:", font=F_NORMAL,
                 bg=C["fondo"], fg=C["texto_suave"]).pack(side="left")

        self.filtro_producto = tk.StringVar(value="Todos")
        try:
            prods = obtener_productos()
        except Exception:
            prods = []
        combo = ttk.Combobox(
            filtros, textvariable=self.filtro_producto,
            values=["Todos"] + prods,
            state="readonly", font=F_NORMAL, width=20)
        combo.pack(side="left", padx=10)
        combo.bind("<<ComboboxSelected>>", lambda _: self._cargar())

        tk.Label(filtros, text="Ultimos:", font=F_NORMAL,
                 bg=C["fondo"], fg=C["texto_suave"]
                 ).pack(side="left", padx=(20, 5))

        self.dias_var = tk.IntVar(value=30)
        for dias in (7, 14, 30, 60):
            btn = tk.Radiobutton(
                filtros, text=f"{dias} dias", variable=self.dias_var,
                value=dias, command=self._cargar,
                bg=C["fondo"], fg=C["texto"], font=F_NORMAL,
                selectcolor=C["seleccion"],
                activebackground=C["fondo"])
            btn.pack(side="left", padx=6)

        tabla_frame = tk.Frame(self, bg=C["fondo"], padx=25)
        tabla_frame.pack(fill="both", expand=True, pady=10)

        columnas = ("fecha", "dia", "producto", "producido", "vendido", "sobrante")
        self.tabla = ttk.Treeview(
            tabla_frame, columns=columnas, show="headings", height=16)

        encabezados = {
            "fecha": ("Fecha", 110), "dia": ("Dia", 100),
            "producto": ("Producto", 150), "producido": ("Horneados", 100),
            "vendido": ("Vendidos", 100), "sobrante": ("Sobrante", 100),
        }
        for col, (titulo, ancho) in encabezados.items():
            self.tabla.heading(col, text=titulo)
            self.tabla.column(col, width=ancho, anchor="center")

        scrollbar = ttk.Scrollbar(tabla_frame, orient="vertical",
                                   command=self.tabla.yview)
        self.tabla.configure(yscrollcommand=scrollbar.set)

        self.tabla.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self._cargar()

    def _cargar(self):
        for row in self.tabla.get_children():
            self.tabla.delete(row)

        producto = self.filtro_producto.get()
        dias = self.dias_var.get()
        try:
            registros = obtener_registros(
                producto if producto != "Todos" else None, dias=dias)
        except Exception:
            registros = []

        for r in registros:
            sobrante = r["sobrante"]
            tag = "normal"
            if sobrante < 0:
                tag = "negativo"
            elif r["producido"] > 0 and sobrante / r["producido"] > 0.15:
                tag = "alto_sobrante"

            self.tabla.insert("", "end", values=(
                r["fecha"], r["dia_semana"], f"{_get_emoji(r['producto'])} {r['producto']}",
                r["producido"], r["vendido"], sobrante
            ), tags=(tag,))

        self.tabla.tag_configure("alto_sobrante",
                                  background=C["amarillo_claro"],
                                  foreground=C["amarillo"])
        self.tabla.tag_configure("negativo",
                                  background=C["rojo_claro"],
                                  foreground=C["rojo"])


class VistaConfiguracion(ttk.Frame):
    """Configuracion de productos, precios y usuarios."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        canvas = tk.Canvas(self, bg=C["fondo"], highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        scroll_frame = tk.Frame(canvas, bg=C["fondo"])

        scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        canvas.bind_all("<MouseWheel>",
                         lambda e: canvas.yview_scroll(
                             int(-1 * (e.delta / 120)), "units"))

        tk.Label(scroll_frame, text="\u2699\uFE0F Configuracion",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]
                 ).pack(fill="x", padx=25, pady=(20, 10))

        self._seccion_productos(scroll_frame)
        self._seccion_usuarios(scroll_frame)
        self._seccion_info(scroll_frame)

    def _seccion_productos(self, parent):
        sec = _seccion(parent, "\U0001F35E Productos y Precios")

        self.productos_frame = tk.Frame(sec, bg=C["tarjeta"])
        self.productos_frame.pack(fill="x", pady=5)

        self._cargar_productos()

        add_frame = tk.Frame(sec, bg=C["tarjeta"], pady=10)
        add_frame.pack(fill="x")

        tk.Label(add_frame, text="Nuevo producto:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(side="left")

        self.nuevo_nombre = tk.StringVar()
        tk.Entry(add_frame, textvariable=self.nuevo_nombre,
                 font=F_NORMAL, bg=C["fondo"], fg=C["texto"],
                 relief="solid", bd=1, width=18).pack(side="left", padx=8)

        tk.Label(add_frame, text="Precio $:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(side="left")

        self.nuevo_precio = tk.StringVar(value="10.0")
        tk.Entry(add_frame, textvariable=self.nuevo_precio,
                 font=F_NORMAL, bg=C["fondo"], fg=C["texto"],
                 relief="solid", bd=1, width=8).pack(side="left", padx=8)

        ttk.Button(add_frame, text="Agregar",
                   style="Primario.TButton",
                   command=self._agregar_producto).pack(side="left", padx=5)

    def _cargar_productos(self):
        for w in self.productos_frame.winfo_children():
            w.destroy()

        try:
            productos = obtener_productos_con_precio()
        except Exception:
            productos = []

        for p in productos:
            emoji = _get_emoji(p["nombre"])
            fila = tk.Frame(self.productos_frame, bg=C["tarjeta"])
            fila.pack(fill="x", pady=3)

            tk.Label(fila, text=f"{emoji} {p['nombre']}", font=F_GRANDE_B,
                     bg=C["tarjeta"], fg=C["texto"],
                     width=22, anchor="w").pack(side="left", padx=5)

            tk.Label(fila, text=f"${p['precio']:.2f}", font=F_GRANDE,
                     bg=C["tarjeta"], fg=C["verde"],
                     width=10).pack(side="left")

            precio_entry = tk.StringVar(value=str(p["precio"]))
            tk.Entry(fila, textvariable=precio_entry,
                     font=F_NORMAL, width=8, bg=C["fondo"],
                     relief="solid", bd=1).pack(side="left", padx=5)

            ttk.Button(
                fila, text="Cambiar Precio",
                style="Secundario.TButton",
                command=lambda nombre=p["nombre"], var=precio_entry:
                    self._cambiar_precio(nombre, var)
            ).pack(side="left", padx=5)

    def _cambiar_precio(self, nombre, precio_var):
        try:
            nuevo = float(precio_var.get())
            if nuevo < 0:
                messagebox.showwarning("Atencion", "El precio no puede ser negativo.")
                return
            if actualizar_precio(nombre, nuevo):
                self._cargar_productos()
                messagebox.showinfo("Listo", f"Precio de '{nombre}' actualizado a ${nuevo:.2f}")
            else:
                messagebox.showerror("Error", "No se pudo actualizar.")
        except ValueError:
            messagebox.showerror("Error", "Escribe un numero valido para el precio.")
        except Exception as e:
            messagebox.showerror("Error", f"Error inesperado: {e}")

    def _agregar_producto(self):
        nombre = self.nuevo_nombre.get().strip()
        if not nombre:
            messagebox.showwarning("Atencion", "Escribe el nombre del producto.")
            return
        try:
            precio = float(self.nuevo_precio.get())
        except ValueError:
            messagebox.showerror("Error", "Precio invalido.")
            return

        try:
            if agregar_producto(nombre, precio):
                self._cargar_productos()
                self.nuevo_nombre.set("")
                messagebox.showinfo("Listo", f"Producto '{nombre}' agregado.")
            else:
                messagebox.showwarning("Atencion", "Ese producto ya existe.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo agregar: {e}")

    def _seccion_usuarios(self, parent):
        sec = _seccion(parent, "\U0001F465 Usuarios")

        self.usuarios_frame = tk.Frame(sec, bg=C["tarjeta"])
        self.usuarios_frame.pack(fill="x", pady=5)

        self._cargar_usuarios()

        add_frame = tk.Frame(sec, bg=C["tarjeta"], pady=10)
        add_frame.pack(fill="x")

        tk.Label(add_frame, text="Nombre:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(side="left")

        self.nuevo_user_nombre = tk.StringVar()
        tk.Entry(add_frame, textvariable=self.nuevo_user_nombre,
                 font=F_NORMAL, bg=C["fondo"], relief="solid", bd=1,
                 width=14).pack(side="left", padx=5)

        tk.Label(add_frame, text="PIN:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(side="left")

        self.nuevo_user_pin = tk.StringVar()
        tk.Entry(add_frame, textvariable=self.nuevo_user_pin,
                 font=F_NORMAL, bg=C["fondo"], relief="solid", bd=1,
                 width=8).pack(side="left", padx=5)

        tk.Label(add_frame, text="Rol:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(side="left")

        self.nuevo_user_rol = tk.StringVar(value="cajero")
        ttk.Combobox(add_frame, textvariable=self.nuevo_user_rol,
                      values=["cajero", "panadero"], state="readonly",
                      font=F_NORMAL, width=10).pack(side="left", padx=5)

        ttk.Button(add_frame, text="Agregar",
                   style="Primario.TButton",
                   command=self._agregar_usuario).pack(side="left", padx=5)

    def _cargar_usuarios(self):
        for w in self.usuarios_frame.winfo_children():
            w.destroy()

        try:
            usuarios = obtener_usuarios()
        except Exception:
            usuarios = []

        for u in usuarios:
            fila = tk.Frame(self.usuarios_frame, bg=C["tarjeta"])
            fila.pack(fill="x", pady=3)

            rol_color = C["secundario"] if u["rol"] == "panadero" else C["primario"]
            tk.Label(fila, text=f"[{u['rol'].upper()}]", font=F_NORMAL_B,
                     bg=C["tarjeta"], fg=rol_color, width=12).pack(side="left")

            tk.Label(fila, text=u["nombre"], font=F_GRANDE,
                     bg=C["tarjeta"], fg=C["texto"]).pack(side="left", padx=10)

            ttk.Button(fila, text="Eliminar",
                       style="Rojo.TButton",
                       command=lambda uid=u["id"]:
                           self._eliminar_usuario(uid)
                       ).pack(side="right", padx=5)

    def _agregar_usuario(self):
        nombre = self.nuevo_user_nombre.get().strip()
        pin = self.nuevo_user_pin.get().strip()
        rol = self.nuevo_user_rol.get()

        if not nombre or not pin:
            messagebox.showwarning("Atencion", "Llena nombre y PIN.")
            return

        try:
            if agregar_usuario(nombre, pin, rol):
                self._cargar_usuarios()
                self.nuevo_user_nombre.set("")
                self.nuevo_user_pin.set("")
                messagebox.showinfo("Listo", f"Usuario '{nombre}' agregado como {rol}.")
            else:
                messagebox.showerror("Error", "No se pudo agregar el usuario.")
        except Exception as e:
            messagebox.showerror("Error", f"Error: {e}")

    def _eliminar_usuario(self, uid):
        if messagebox.askyesno("Confirmar",
                                "Seguro que quieres eliminar este usuario?"):
            try:
                if eliminar_usuario(uid):
                    self._cargar_usuarios()
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo eliminar: {e}")

    def _seccion_info(self, parent):
        sec = _seccion(parent, "\u2139\uFE0F Acerca del Sistema")

        info = [
            ("Version",  "3.0.0 \u2728"),
            ("Motor",    "Python + SQLite + Matplotlib"),
            ("Modelos",  "Estimacion \u2192 Promedio Semanal \u2192 Por Dia"),
        ]
        for k, v in info:
            fila = tk.Frame(sec, bg=C["tarjeta"])
            fila.pack(fill="x", pady=3)
            tk.Label(fila, text=f"{k}:", font=F_NORMAL,
                     bg=C["tarjeta"], fg=C["texto_suave"],
                     width=15, anchor="w").pack(side="left")
            tk.Label(fila, text=v, font=F_NORMAL_B,
                     bg=C["tarjeta"], fg=C["primario"]).pack(side="left")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# WIDGETS REUTILIZABLES
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _tarjeta_kpi(parent, titulo: str, valor: str, color: str):
    """Tarjeta KPI premium con borde de color."""
    outer = tk.Frame(parent, bg=C["sombra"], padx=1, pady=1)
    outer.pack(side="left", padx=8, fill="y")

    card = tk.Frame(outer, bg=C["tarjeta"], padx=25, pady=15)
    card.pack(fill="both", expand=True)

    # Accent bar superior
    tk.Frame(card, bg=color, height=3).pack(fill="x", pady=(0, 8))

    tk.Label(card, text=titulo, font=F_NORMAL,
             bg=C["tarjeta"], fg=C["texto_suave"]).pack()
    tk.Label(card, text=valor, font=(FONT_FAMILY, 26, "bold"),
             bg=C["tarjeta"], fg=color).pack(pady=(5, 0))


def _seccion(parent, titulo: str) -> tk.Frame:
    """Seccion con titulo y contenedor premium."""
    tk.Label(parent, text=titulo, font=F_SUBTIT,
             bg=C["fondo"], fg=C["secundario"],
             padx=25, pady=(10), anchor="w").pack(fill="x")

    outer = tk.Frame(parent, bg=C["sombra"], padx=1, pady=1)
    outer.pack(fill="x", padx=25, pady=(0, 15))

    contenedor = tk.Frame(outer, bg=C["tarjeta"], padx=20, pady=15)
    contenedor.pack(fill="x")
    return contenedor


def _campo_label(parent, texto: str, fila: int):
    tk.Label(parent, text=texto, font=F_NORMAL,
             bg=C["tarjeta"], fg=C["texto_suave"],
             anchor="w").grid(row=fila, column=0, sticky="w",
                               pady=10, padx=(0, 15))


def _campo_entry(parent, var: tk.StringVar, fila: int) -> tk.Entry:
    e = tk.Entry(parent, textvariable=var, font=F_GRANDE,
                 bg=C["fondo"], fg=C["texto"],
                 insertbackground=C["primario"],
                 relief="solid", bd=1, width=28,
                 highlightthickness=2,
                 highlightbackground=C["borde"],
                 highlightcolor=C["dorado"])
    e.grid(row=fila, column=1, sticky="ew", pady=10)
    return e


def _mostrar_error_vista(parent, error):
    """Muestra un error amigable en la vista."""
    frame = tk.Frame(parent, bg=C["fondo"])
    frame.pack(fill="both", expand=True)
    tk.Label(frame, text="\u26A0\uFE0F", font=(FONT_FAMILY, 48),
             bg=C["fondo"]).pack(pady=(40, 10))
    tk.Label(frame, text="Ocurrio un problema al cargar esta seccion.",
             font=F_GRANDE_B, bg=C["fondo"], fg=C["rojo"]).pack()
    tk.Label(frame, text=f"Detalle: {error}",
             font=F_NORMAL, bg=C["fondo"], fg=C["texto_suave"]).pack(pady=10)
    tk.Label(frame, text="Intenta cambiar de seccion o cerrar sesion.",
             font=F_NORMAL, bg=C["fondo"], fg=C["texto_suave"]).pack()


def _mostrar_error_mini(parent, producto, error):
    """Error pequeno para tarjeta de producto."""
    card = tk.Frame(parent, bg=C["rojo_claro"], padx=15, pady=10)
    card.pack(fill="x", padx=25, pady=5)
    tk.Label(card, text=f"\u26A0 Error en {producto}: {error}",
             font=F_NORMAL, bg=C["rojo_claro"], fg=C["rojo"]).pack(anchor="w")


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Punto de entrada
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

if __name__ == "__main__":
    app = PanaderiaApp()
    app.mainloop()
