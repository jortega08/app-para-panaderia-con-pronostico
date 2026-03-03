"""
app.py
------
Interfaz grafica de Panaderia - Sistema de Pronostico y Punto de Venta.

Roles:
  - Panadero: ve pronosticos, registra produccion, configura productos
  - Cajero: registra ventas, ve resumen del dia

Disenado para personas mayores: fuentes grandes, botones amplios,
colores claros, lenguaje sencillo.
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
from typing import Optional
import sys
import os

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


# ──────────────────────────────────────────────
# Tema claro - Colores calidos de panaderia
# ──────────────────────────────────────────────
C = {
    "fondo":        "#FFF8F0",
    "fondo_nav":    "#F5E6D3",
    "tarjeta":      "#FFFFFF",
    "primario":     "#D4722A",
    "primario_hover": "#B85D1F",
    "secundario":   "#8B5E3C",
    "texto":        "#2C1810",
    "texto_suave":  "#7A6455",
    "verde":        "#2E7D32",
    "verde_claro":  "#E8F5E9",
    "amarillo":     "#F57F17",
    "amarillo_claro": "#FFF8E1",
    "rojo":         "#C62828",
    "rojo_claro":   "#FFEBEE",
    "borde":        "#E0D5C8",
    "seleccion":    "#FFE0B2",
}

# Fuentes grandes para facilidad de lectura
F_TITULO   = ("Arial", 24, "bold")
F_SUBTIT   = ("Arial", 18, "bold")
F_GRANDE   = ("Arial", 16)
F_GRANDE_B = ("Arial", 16, "bold")
F_NORMAL   = ("Arial", 14)
F_NORMAL_B = ("Arial", 14, "bold")
F_BOTON    = ("Arial", 15, "bold")
F_NUMERO   = ("Arial", 36, "bold")
F_PEQUENA  = ("Arial", 11)


# ══════════════════════════════════════════════
# APLICACION PRINCIPAL
# ══════════════════════════════════════════════

class PanaderiaApp(tk.Tk):
    """Ventana principal."""

    def __init__(self):
        super().__init__()
        self.title("Panaderia - Sistema de Ventas y Pronostico")
        self.geometry("1100x750")
        self.minsize(900, 650)
        self.configure(bg=C["fondo"])

        inicializar_base_de_datos()

        self.usuario_actual = None  # {nombre, pin, rol}
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

        # Boton principal (naranja)
        s.configure("Primario.TButton",
                     background=C["primario"], foreground="white",
                     font=F_BOTON, borderwidth=0, padding=(20, 12))
        s.map("Primario.TButton",
              background=[("active", C["primario_hover"])])

        # Boton secundario (cafe)
        s.configure("Secundario.TButton",
                     background=C["secundario"], foreground="white",
                     font=F_NORMAL_B, borderwidth=0, padding=(16, 10))
        s.map("Secundario.TButton",
              background=[("active", "#6D4930")])

        # Boton de navegacion
        s.configure("Nav.TButton",
                     background=C["fondo_nav"], foreground=C["texto_suave"],
                     font=F_GRANDE, borderwidth=0, padding=(16, 14))
        s.map("Nav.TButton",
              background=[("active", C["seleccion"])],
              foreground=[("active", C["primario"])])

        # Boton de navegacion activo
        s.configure("NavActivo.TButton",
                     background=C["seleccion"], foreground=C["primario"],
                     font=F_GRANDE_B, borderwidth=0, padding=(16, 14))

        # Boton verde (registrar venta)
        s.configure("Verde.TButton",
                     background=C["verde"], foreground="white",
                     font=F_BOTON, borderwidth=0, padding=(20, 14))
        s.map("Verde.TButton",
              background=[("active", "#1B5E20")])

        # Boton rojo
        s.configure("Rojo.TButton",
                     background=C["rojo"], foreground="white",
                     font=F_NORMAL_B, borderwidth=0, padding=(14, 8))
        s.map("Rojo.TButton",
              background=[("active", "#8E1A1A")])

        # Boton producto POS (grande)
        s.configure("Producto.TButton",
                     background=C["tarjeta"], foreground=C["texto"],
                     font=F_GRANDE_B, borderwidth=2, padding=(10, 20),
                     relief="solid")
        s.map("Producto.TButton",
              background=[("active", C["seleccion"])])

        # Tabla
        s.configure("Treeview", background=C["tarjeta"],
                     foreground=C["texto"], fieldbackground=C["tarjeta"],
                     font=F_NORMAL, rowheight=36)
        s.configure("Treeview.Heading",
                     background=C["fondo_nav"], foreground=C["secundario"],
                     font=F_NORMAL_B)
        s.map("Treeview",
              background=[("selected", C["seleccion"])],
              foreground=[("selected", C["texto"])])

        # Combobox
        s.configure("TCombobox", fieldbackground=C["tarjeta"],
                     background=C["tarjeta"], foreground=C["texto"],
                     font=F_NORMAL)

        self.option_add("*TCombobox*Listbox.font", F_NORMAL)
        self.option_add("*TCombobox*Listbox.background", C["tarjeta"])
        self.option_add("*TCombobox*Listbox.foreground", C["texto"])

    # ──────────────────────────────────────────
    # Login
    # ──────────────────────────────────────────

    def _mostrar_login(self):
        """Pantalla de inicio de sesion con PIN."""
        for w in self.winfo_children():
            w.destroy()

        self.usuario_actual = None

        frame = tk.Frame(self, bg=C["fondo"])
        frame.place(relx=0.5, rely=0.5, anchor="center")

        # Logo
        tk.Label(frame, text="PANADERIA", font=("Arial", 40, "bold"),
                 bg=C["fondo"], fg=C["primario"]).pack(pady=(0, 5))
        tk.Label(frame, text="Sistema de Ventas y Pronostico",
                 font=F_GRANDE, bg=C["fondo"], fg=C["texto_suave"]).pack()

        tk.Frame(frame, bg=C["borde"], height=2).pack(fill="x", pady=30, padx=40)

        tk.Label(frame, text="Ingresa tu PIN para entrar:",
                 font=F_GRANDE, bg=C["fondo"], fg=C["texto"]).pack(pady=(0, 15))

        # Campo PIN
        pin_frame = tk.Frame(frame, bg=C["fondo"])
        pin_frame.pack(pady=10)

        self.pin_var = tk.StringVar()
        self.pin_entry = tk.Entry(
            pin_frame, textvariable=self.pin_var,
            font=("Arial", 32, "bold"), width=8,
            justify="center", show="*",
            bg=C["tarjeta"], fg=C["texto"],
            insertbackground=C["primario"],
            relief="solid", bd=2,
            highlightthickness=2,
            highlightcolor=C["primario"],
            highlightbackground=C["borde"]
        )
        self.pin_entry.pack(pady=5)
        self.pin_entry.focus_set()
        self.pin_entry.bind("<Return>", lambda _: self._intentar_login())

        # Teclado numerico en pantalla (para facilidad)
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
                        fila, text=num, font=F_BOTON, width=6, height=2,
                        bg=C["verde"], fg="white", relief="flat",
                        activebackground="#1B5E20",
                        command=self._intentar_login
                    )
                elif num == "Borrar":
                    btn = tk.Button(
                        fila, text=num, font=F_BOTON, width=6, height=2,
                        bg=C["rojo"], fg="white", relief="flat",
                        activebackground="#8E1A1A",
                        command=lambda: self.pin_var.set(self.pin_var.get()[:-1])
                    )
                else:
                    btn = tk.Button(
                        fila, text=num, font=("Arial", 20, "bold"),
                        width=6, height=2,
                        bg=C["tarjeta"], fg=C["texto"], relief="solid",
                        bd=1, activebackground=C["seleccion"],
                        command=lambda n=num: self.pin_var.set(
                            self.pin_var.get() + n)
                    )
                btn.pack(side="left", padx=3, pady=3)

        # Mensaje de error
        self.login_msg = tk.Label(frame, text="", font=F_NORMAL,
                                   bg=C["fondo"], fg=C["rojo"])
        self.login_msg.pack(pady=10)

        # Info de PINs por defecto
        tk.Label(frame, text="PIN Panadero: 1234  |  PIN Cajero: 0000",
                 font=F_PEQUENA, bg=C["fondo"],
                 fg=C["texto_suave"]).pack(pady=(10, 0))

    def _intentar_login(self):
        pin = self.pin_var.get().strip()
        if not pin:
            self.login_msg.configure(text="Escribe tu PIN")
            return

        usuario = verificar_pin(pin)
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

    # ──────────────────────────────────────────
    # INTERFAZ CAJERO
    # ──────────────────────────────────────────

    def _construir_cajero(self):
        # Barra superior
        top = tk.Frame(self, bg=C["primario"], height=60)
        top.pack(fill="x")
        top.pack_propagate(False)

        tk.Label(top, text=f"Cajero: {self.usuario_actual['nombre']}",
                 font=F_GRANDE_B, bg=C["primario"], fg="white"
                 ).pack(side="left", padx=20)

        tk.Button(top, text="Cerrar Sesion", font=F_NORMAL_B,
                  bg=C["primario_hover"], fg="white", relief="flat",
                  activebackground=C["rojo"],
                  command=self._mostrar_login
                  ).pack(side="right", padx=20, pady=10)

        # Pestanas simples
        self.cajero_nav = tk.Frame(self, bg=C["fondo_nav"])
        self.cajero_nav.pack(fill="x")

        self.cajero_content = tk.Frame(self, bg=C["fondo"])
        self.cajero_content.pack(fill="both", expand=True)

        self._cajero_tabs = {}
        for texto, vista in [("Registrar Venta", "pos"),
                              ("Ventas de Hoy", "resumen")]:
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

        if nombre == "pos":
            VistaPOS(self.cajero_content, self).pack(fill="both", expand=True)
        elif nombre == "resumen":
            VistaResumenDia(self.cajero_content, self).pack(
                fill="both", expand=True)

    # ──────────────────────────────────────────
    # INTERFAZ PANADERO
    # ──────────────────────────────────────────

    def _construir_panadero(self):
        # Barra superior
        top = tk.Frame(self, bg=C["secundario"], height=60)
        top.pack(fill="x")
        top.pack_propagate(False)

        tk.Label(top, text=f"Panadero: {self.usuario_actual['nombre']}",
                 font=F_GRANDE_B, bg=C["secundario"], fg="white"
                 ).pack(side="left", padx=20)

        tk.Button(top, text="Cerrar Sesion", font=F_NORMAL_B,
                  bg="#6D4930", fg="white", relief="flat",
                  activebackground=C["rojo"],
                  command=self._mostrar_login
                  ).pack(side="right", padx=20, pady=10)

        # Navegacion lateral
        body = tk.Frame(self, bg=C["fondo"])
        body.pack(fill="both", expand=True)

        nav = tk.Frame(body, bg=C["fondo_nav"], width=220)
        nav.pack(side="left", fill="y")
        nav.pack_propagate(False)

        self.panadero_content = tk.Frame(body, bg=C["fondo"])
        self.panadero_content.pack(side="left", fill="both", expand=True)

        self._panadero_tabs = {}
        vistas = [
            ("Cuantos Hornear", "pronostico"),
            ("Registrar Produccion", "produccion"),
            ("Ventas de Hoy", "ventas"),
            ("Historial", "historial"),
            ("Configuracion", "config"),
        ]
        for texto, vista in vistas:
            btn = ttk.Button(nav, text=texto, style="Nav.TButton",
                             command=lambda v=vista: self._panadero_vista(v))
            btn.pack(fill="x", padx=8, pady=3)
            self._panadero_tabs[vista] = btn

        # Fecha actual abajo
        tk.Label(nav, text=datetime.now().strftime("%d/%m/%Y"),
                 font=F_GRANDE, bg=C["fondo_nav"],
                 fg=C["texto_suave"]).pack(side="bottom", pady=20)

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
        if nombre in vistas:
            vistas[nombre](self.panadero_content, self).pack(
                fill="both", expand=True)


# ══════════════════════════════════════════════
# VISTAS DEL CAJERO
# ══════════════════════════════════════════════

class VistaPOS(ttk.Frame):
    """Punto de venta simplificado. Botones grandes por producto."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        # Titulo
        header = tk.Frame(self, bg=C["fondo"])
        header.pack(fill="x", padx=25, pady=(20, 10))

        tk.Label(header, text="Registrar Venta",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]).pack(side="left")

        # Area principal: productos a la izquierda, resumen a la derecha
        main = tk.Frame(self, bg=C["fondo"])
        main.pack(fill="both", expand=True, padx=25, pady=10)

        # --- Panel de productos ---
        prod_frame = tk.Frame(main, bg=C["fondo"])
        prod_frame.pack(side="left", fill="both", expand=True, padx=(0, 15))

        tk.Label(prod_frame, text="Selecciona el producto:",
                 font=F_GRANDE, bg=C["fondo"],
                 fg=C["texto_suave"]).pack(anchor="w", pady=(0, 10))

        self.productos = obtener_productos_con_precio()
        self.producto_seleccionado = None

        # Grid de botones de productos
        grid = tk.Frame(prod_frame, bg=C["fondo"])
        grid.pack(fill="both", expand=True)

        self._botones_producto = []
        for i, p in enumerate(self.productos):
            btn_frame = tk.Frame(grid, bg=C["borde"], bd=2, relief="solid")
            fila = i // 2
            col = i % 2
            btn_frame.grid(row=fila, column=col, padx=8, pady=8, sticky="nsew")
            grid.grid_columnconfigure(col, weight=1)
            grid.grid_rowconfigure(fila, weight=1)

            btn = tk.Button(
                btn_frame,
                text=f"{p['nombre']}\n${p['precio']:.2f}",
                font=F_GRANDE_B, bg=C["tarjeta"], fg=C["texto"],
                relief="flat", activebackground=C["seleccion"],
                cursor="hand2",
                command=lambda prod=p: self._seleccionar_producto(prod)
            )
            btn.pack(fill="both", expand=True, padx=2, pady=2)
            self._botones_producto.append((btn, p))

        # --- Panel de venta ---
        venta_frame = tk.Frame(main, bg=C["tarjeta"], bd=2, relief="solid",
                                padx=25, pady=20, width=320)
        venta_frame.pack(side="right", fill="y")
        venta_frame.pack_propagate(False)

        tk.Label(venta_frame, text="Detalle de Venta",
                 font=F_SUBTIT, bg=C["tarjeta"],
                 fg=C["secundario"]).pack(pady=(0, 15))

        # Producto seleccionado
        tk.Label(venta_frame, text="Producto:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(anchor="w")
        self.lbl_producto = tk.Label(venta_frame, text="(ninguno)",
                                      font=F_GRANDE_B, bg=C["tarjeta"],
                                      fg=C["primario"])
        self.lbl_producto.pack(anchor="w", pady=(0, 15))

        # Cantidad
        tk.Label(venta_frame, text="Cantidad:", font=F_NORMAL,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(anchor="w")

        cant_frame = tk.Frame(venta_frame, bg=C["tarjeta"])
        cant_frame.pack(fill="x", pady=(5, 15))

        self.cantidad_var = tk.IntVar(value=1)

        tk.Button(cant_frame, text=" - ", font=("Arial", 22, "bold"),
                  bg=C["rojo_claro"], fg=C["rojo"], relief="flat", width=3,
                  command=self._decrementar).pack(side="left")

        self.lbl_cantidad = tk.Label(cant_frame,
                                      textvariable=self.cantidad_var,
                                      font=F_NUMERO, bg=C["tarjeta"],
                                      fg=C["texto"], width=4)
        self.lbl_cantidad.pack(side="left", expand=True)

        tk.Button(cant_frame, text=" + ", font=("Arial", 22, "bold"),
                  bg=C["verde_claro"], fg=C["verde"], relief="flat", width=3,
                  command=self._incrementar).pack(side="left")

        # Separador
        tk.Frame(venta_frame, bg=C["borde"], height=2).pack(fill="x", pady=10)

        # Total
        tk.Label(venta_frame, text="Total:", font=F_GRANDE,
                 bg=C["tarjeta"], fg=C["texto_suave"]).pack(anchor="w")
        self.lbl_total = tk.Label(venta_frame, text="$0.00",
                                   font=("Arial", 32, "bold"),
                                   bg=C["tarjeta"], fg=C["verde"])
        self.lbl_total.pack(anchor="w", pady=(5, 20))

        # Boton registrar
        self.btn_registrar = ttk.Button(
            venta_frame, text="Registrar Venta",
            style="Verde.TButton",
            command=self._registrar_venta
        )
        self.btn_registrar.pack(fill="x", pady=(10, 0))

        self._actualizar_total()

    def _seleccionar_producto(self, producto):
        self.producto_seleccionado = producto
        self.lbl_producto.configure(text=producto["nombre"])
        # Resaltar boton seleccionado
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

        exito = registrar_venta(producto, cantidad, precio, usuario)
        if exito:
            total = precio * cantidad
            messagebox.showinfo(
                "Venta Registrada",
                f"{cantidad}x {producto}\n"
                f"Total: ${total:.2f}\n\n"
                f"Registrado correctamente."
            )
            # Resetear
            self.cantidad_var.set(1)
            self.producto_seleccionado = None
            self.lbl_producto.configure(text="(ninguno)")
            for btn, _ in self._botones_producto:
                btn.configure(bg=C["tarjeta"])
            self._actualizar_total()
        else:
            messagebox.showerror("Error", "No se pudo registrar la venta.")


class VistaResumenDia(ttk.Frame):
    """Resumen de ventas del dia para el cajero."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        header = tk.Frame(self, bg=C["fondo"])
        header.pack(fill="x", padx=25, pady=(20, 10))

        tk.Label(header, text="Ventas de Hoy",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]).pack(side="left")

        ttk.Button(header, text="Actualizar", style="Primario.TButton",
                   command=self._actualizar).pack(side="right")

        # Tarjetas resumen
        self.resumen_frame = tk.Frame(self, bg=C["fondo"])
        self.resumen_frame.pack(fill="x", padx=25, pady=10)

        # Tabla de detalle
        self.tabla_frame = tk.Frame(self, bg=C["fondo"])
        self.tabla_frame.pack(fill="both", expand=True, padx=25, pady=10)

        self._actualizar()

    def _actualizar(self):
        for w in self.resumen_frame.winfo_children():
            w.destroy()
        for w in self.tabla_frame.winfo_children():
            w.destroy()

        totales = obtener_total_ventas_dia()
        resumen = obtener_resumen_ventas_dia()

        # Tarjetas de totales
        cards = tk.Frame(self.resumen_frame, bg=C["fondo"])
        cards.pack(fill="x")

        _tarjeta(cards, "Total Vendido",
                 f"${totales['dinero']:.2f}", C["verde"])
        _tarjeta(cards, "Panes Vendidos",
                 str(totales["panes"]), C["primario"])
        _tarjeta(cards, "Transacciones",
                 str(totales["transacciones"]), C["secundario"])

        # Tabla por producto
        if resumen:
            tk.Label(self.tabla_frame, text="Detalle por Producto",
                     font=F_SUBTIT, bg=C["fondo"],
                     fg=C["secundario"]).pack(anchor="w", pady=(10, 5))

            cols = ("producto", "cantidad", "total")
            tabla = ttk.Treeview(self.tabla_frame, columns=cols,
                                  show="headings", height=10)
            tabla.heading("producto", text="Producto")
            tabla.heading("cantidad", text="Cantidad")
            tabla.heading("total", text="Total $")
            tabla.column("producto", width=200)
            tabla.column("cantidad", width=120, anchor="center")
            tabla.column("total", width=150, anchor="center")

            for r in resumen:
                tabla.insert("", "end", values=(
                    r["producto"],
                    r["total_cantidad"],
                    f"${r['total_dinero']:.2f}"
                ))

            tabla.pack(fill="both", expand=True)
        else:
            tk.Label(self.tabla_frame,
                     text="No hay ventas registradas hoy.\n\n"
                          "Ve a 'Registrar Venta' para comenzar.",
                     font=F_GRANDE, bg=C["fondo"],
                     fg=C["texto_suave"]).pack(expand=True)


# ══════════════════════════════════════════════
# VISTAS DEL PANADERO
# ══════════════════════════════════════════════

class VistaPronostico(ttk.Frame):
    """Dashboard simplificado: cuantos panes hornear."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        # Scroll
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

        # Bind mousewheel
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
        canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))

        # Contenido
        header = tk.Frame(self.scroll_frame, bg=C["fondo"])
        header.pack(fill="x", padx=25, pady=(20, 5))

        tk.Label(header, text="Cuantos Hornear Hoy",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]).pack(side="left")

        fecha_hoy = datetime.now().strftime("%d/%m/%Y")
        tk.Label(header, text=fecha_hoy, font=F_GRANDE,
                 bg=C["fondo"], fg=C["texto_suave"]).pack(side="right")

        # Pronostico por cada producto
        productos = obtener_productos()

        if not productos:
            tk.Label(self.scroll_frame,
                     text="No hay productos. Ve a Configuracion para agregar.",
                     font=F_GRANDE, bg=C["fondo"],
                     fg=C["texto_suave"]).pack(pady=50)
            return

        for producto in productos:
            self._tarjeta_pronostico(self.scroll_frame, producto)

    def _tarjeta_pronostico(self, parent, producto: str):
        resultado = calcular_pronostico(producto)
        registros = obtener_registros(producto, dias=7)
        eficiencia = calcular_eficiencia(registros)
        tendencia = analizar_tendencia(registros)

        # Color del borde segun estado
        color_estado = {
            "bien": C["verde"],
            "alerta": C["amarillo"],
            "problema": C["rojo"],
        }.get(resultado.estado, C["texto_suave"])

        color_fondo = {
            "bien": C["verde_claro"],
            "alerta": C["amarillo_claro"],
            "problema": C["rojo_claro"],
        }.get(resultado.estado, C["fondo"])

        # Tarjeta
        card = tk.Frame(parent, bg=color_estado, padx=3, pady=3)
        card.pack(fill="x", padx=25, pady=8)

        inner = tk.Frame(card, bg=C["tarjeta"], padx=20, pady=15)
        inner.pack(fill="x")

        # Fila 1: Nombre del producto + Cantidad sugerida
        fila1 = tk.Frame(inner, bg=C["tarjeta"])
        fila1.pack(fill="x")

        tk.Label(fila1, text=producto, font=F_SUBTIT,
                 bg=C["tarjeta"], fg=C["texto"]).pack(side="left")

        # Numero grande de produccion sugerida
        num_frame = tk.Frame(fila1, bg=color_fondo, padx=15, pady=5)
        num_frame.pack(side="right")

        tk.Label(num_frame, text=f"{resultado.produccion_sugerida}",
                 font=("Arial", 30, "bold"), bg=color_fondo,
                 fg=color_estado).pack(side="left")
        tk.Label(num_frame, text=" panes", font=F_GRANDE,
                 bg=color_fondo, fg=color_estado).pack(side="left")

        # Fila 2: Detalles
        fila2 = tk.Frame(inner, bg=C["tarjeta"])
        fila2.pack(fill="x", pady=(10, 0))

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
        msg_frame.pack(fill="x", pady=(10, 0))

        tk.Label(msg_frame, text=resultado.mensaje,
                 font=F_NORMAL, bg=color_fondo,
                 fg=color_estado).pack(anchor="w")


class VistaProduccion(ttk.Frame):
    """Registro de produccion diaria (cuantos se hornearon)."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        tk.Label(self, text="Registrar Produccion del Dia",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"],
                 ).pack(fill="x", padx=25, pady=(20, 10))

        tk.Label(self, text="Registra cuantos panes se hornearon hoy.",
                 font=F_NORMAL, bg=C["fondo"],
                 fg=C["texto_suave"]).pack(padx=25, anchor="w")

        # Formulario
        form_outer = tk.Frame(self, bg=C["fondo"])
        form_outer.pack(expand=True)

        form = tk.Frame(form_outer, bg=C["tarjeta"], padx=35, pady=30,
                         bd=2, relief="solid")
        form.pack(padx=20)

        # Fecha
        _campo_label(form, "Fecha:", 0)
        self.fecha_var = tk.StringVar(
            value=datetime.now().strftime("%Y-%m-%d"))
        _campo_entry(form, self.fecha_var, 0)

        # Producto
        _campo_label(form, "Producto:", 1)
        self.producto_var = tk.StringVar()
        productos = obtener_productos()
        combo = ttk.Combobox(form, textvariable=self.producto_var,
                              values=productos, state="readonly",
                              font=F_GRANDE, width=25)
        if productos:
            combo.current(0)
        combo.grid(row=1, column=1, sticky="ew", pady=10)
        combo.bind("<<ComboboxSelected>>", lambda _: self._actualizar_vendido())

        # Cantidad producida
        _campo_label(form, "Cantidad horneada:", 2)
        self.producido_var = tk.StringVar()
        _campo_entry(form, self.producido_var, 2)

        # Vendido (auto-llenado desde ventas del cajero)
        _campo_label(form, "Cantidad vendida:", 3)
        self.vendido_var = tk.StringVar()
        e_vendido = _campo_entry(form, self.vendido_var, 3)

        # Info de vendido automatico
        self.lbl_auto_vendido = tk.Label(
            form, text="", font=F_PEQUENA,
            bg=C["tarjeta"], fg=C["verde"])
        self.lbl_auto_vendido.grid(row=3, column=2, padx=10)

        # Sobrante calculado
        _campo_label(form, "Sobrante:", 4)
        self.lbl_sobrante = tk.Label(form, text="--", font=F_GRANDE_B,
                                      bg=C["tarjeta"], fg=C["primario"])
        self.lbl_sobrante.grid(row=4, column=1, sticky="w", pady=10)

        # Observaciones
        _campo_label(form, "Notas (opcional):", 5)
        self.obs_var = tk.StringVar()
        _campo_entry(form, self.obs_var, 5)

        # Actualizar sobrante en tiempo real
        for var in (self.producido_var, self.vendido_var):
            var.trace_add("write", self._actualizar_sobrante)

        # Boton guardar
        ttk.Button(form, text="Guardar Registro",
                   style="Primario.TButton",
                   command=self._guardar
                   ).grid(row=6, column=0, columnspan=2, pady=25)

        self._actualizar_vendido()

    def _actualizar_vendido(self):
        """Auto-llenar vendido desde ventas del cajero."""
        fecha = self.fecha_var.get().strip()
        producto = self.producto_var.get()
        if fecha and producto:
            try:
                vendido = obtener_vendido_dia_producto(fecha, producto)
                if vendido > 0:
                    self.vendido_var.set(str(vendido))
                    self.lbl_auto_vendido.configure(
                        text=f"(desde ventas del cajero)")
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
            self.lbl_sobrante.configure(
                text=f"{sobrante} panes", fg=color)
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
                messagebox.showwarning("Atencion",
                                        "Los valores no pueden ser negativos.")
                return
            if vendido > producido:
                messagebox.showwarning(
                    "Atencion",
                    "Se vendio mas de lo que se horneo.\n"
                    "Revisa los datos.")
                return
            datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError as e:
            messagebox.showerror("Error", f"Datos invalidos: {e}")
            return

        exito = guardar_registro(fecha, producto, producido, vendido, obs)
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
    """Vista de ventas del dia para el panadero."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        header = tk.Frame(self, bg=C["fondo"])
        header.pack(fill="x", padx=25, pady=(20, 10))

        tk.Label(header, text="Ventas de Hoy",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]).pack(side="left")

        ttk.Button(header, text="Actualizar", style="Primario.TButton",
                   command=self._actualizar).pack(side="right")

        # Resumen
        self.resumen_frame = tk.Frame(self, bg=C["fondo"])
        self.resumen_frame.pack(fill="x", padx=25)

        # Lista detallada
        self.lista_frame = tk.Frame(self, bg=C["fondo"])
        self.lista_frame.pack(fill="both", expand=True, padx=25, pady=10)

        self._actualizar()

    def _actualizar(self):
        for w in self.resumen_frame.winfo_children():
            w.destroy()
        for w in self.lista_frame.winfo_children():
            w.destroy()

        totales = obtener_total_ventas_dia()
        resumen = obtener_resumen_ventas_dia()
        ventas = obtener_ventas_dia()

        # Tarjetas resumen
        cards = tk.Frame(self.resumen_frame, bg=C["fondo"])
        cards.pack(fill="x", pady=(0, 10))

        _tarjeta(cards, "Total del Dia",
                 f"${totales['dinero']:.2f}", C["verde"])
        _tarjeta(cards, "Panes Vendidos",
                 str(totales["panes"]), C["primario"])
        _tarjeta(cards, "Transacciones",
                 str(totales["transacciones"]), C["secundario"])

        if not ventas:
            tk.Label(self.lista_frame,
                     text="No hay ventas registradas hoy.",
                     font=F_GRANDE, bg=C["fondo"],
                     fg=C["texto_suave"]).pack(pady=30)
            return

        # Resumen por producto
        tk.Label(self.lista_frame, text="Por Producto:",
                 font=F_SUBTIT, bg=C["fondo"],
                 fg=C["secundario"]).pack(anchor="w", pady=(5, 5))

        for r in resumen:
            f = tk.Frame(self.lista_frame, bg=C["tarjeta"],
                          padx=15, pady=10, bd=1, relief="solid")
            f.pack(fill="x", pady=3)
            tk.Label(f, text=r["producto"], font=F_GRANDE_B,
                     bg=C["tarjeta"], fg=C["texto"]).pack(side="left")
            tk.Label(f, text=f"${r['total_dinero']:.2f}",
                     font=F_GRANDE_B, bg=C["tarjeta"],
                     fg=C["verde"]).pack(side="right")
            tk.Label(f, text=f"{r['total_cantidad']} panes  |  ",
                     font=F_NORMAL, bg=C["tarjeta"],
                     fg=C["texto_suave"]).pack(side="right")

        # Detalle de transacciones
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
                v["producto"],
                v["cantidad"],
                f"${v['total']:.2f}"
            ))

        tabla.pack(fill="both", expand=True)


class VistaHistorial(ttk.Frame):
    """Historial de registros de produccion."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent, style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        tk.Label(self, text="Historial de Produccion",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]
                 ).pack(fill="x", padx=25, pady=(20, 10))

        # Filtros
        filtros = tk.Frame(self, bg=C["fondo"], padx=25)
        filtros.pack(fill="x", pady=(0, 10))

        tk.Label(filtros, text="Producto:", font=F_NORMAL,
                 bg=C["fondo"], fg=C["texto_suave"]).pack(side="left")

        self.filtro_producto = tk.StringVar(value="Todos")
        combo = ttk.Combobox(
            filtros, textvariable=self.filtro_producto,
            values=["Todos"] + obtener_productos(),
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

        # Tabla
        tabla_frame = tk.Frame(self, bg=C["fondo"], padx=25)
        tabla_frame.pack(fill="both", expand=True, pady=10)

        columnas = ("fecha", "dia", "producto", "producido",
                    "vendido", "sobrante")
        self.tabla = ttk.Treeview(
            tabla_frame, columns=columnas, show="headings", height=16)

        encabezados = {
            "fecha": ("Fecha", 110),
            "dia": ("Dia", 100),
            "producto": ("Producto", 150),
            "producido": ("Horneados", 100),
            "vendido": ("Vendidos", 100),
            "sobrante": ("Sobrante", 100),
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
        registros = obtener_registros(
            producto if producto != "Todos" else None, dias=dias)

        for r in registros:
            sobrante = r["sobrante"]
            tag = "normal"
            if sobrante < 0:
                tag = "negativo"
            elif r["producido"] > 0 and sobrante / r["producido"] > 0.15:
                tag = "alto_sobrante"

            self.tabla.insert("", "end", values=(
                r["fecha"], r["dia_semana"], r["producto"],
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
        # Scroll
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
        canvas.bind_all("<Button-4>",
                         lambda e: canvas.yview_scroll(-1, "units"))
        canvas.bind_all("<Button-5>",
                         lambda e: canvas.yview_scroll(1, "units"))

        tk.Label(scroll_frame, text="Configuracion",
                 font=F_TITULO, bg=C["fondo"], fg=C["texto"]
                 ).pack(fill="x", padx=25, pady=(20, 10))

        # ── Seccion: Productos y Precios
        self._seccion_productos(scroll_frame)

        # ── Seccion: Usuarios
        self._seccion_usuarios(scroll_frame)

        # ── Info del sistema
        self._seccion_info(scroll_frame)

    def _seccion_productos(self, parent):
        sec = _seccion(parent, "Productos y Precios")

        # Lista de productos con precios
        self.productos_frame = tk.Frame(sec, bg=C["tarjeta"])
        self.productos_frame.pack(fill="x", pady=5)

        self._cargar_productos()

        # Agregar nuevo
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

        productos = obtener_productos_con_precio()
        for p in productos:
            fila = tk.Frame(self.productos_frame, bg=C["tarjeta"])
            fila.pack(fill="x", pady=3)

            tk.Label(fila, text=p["nombre"], font=F_GRANDE_B,
                     bg=C["tarjeta"], fg=C["texto"],
                     width=20, anchor="w").pack(side="left", padx=5)

            tk.Label(fila, text=f"${p['precio']:.2f}", font=F_GRANDE,
                     bg=C["tarjeta"], fg=C["verde"],
                     width=10).pack(side="left")

            # Boton editar precio
            precio_entry = tk.StringVar(value=str(p["precio"]))
            e = tk.Entry(fila, textvariable=precio_entry,
                         font=F_NORMAL, width=8, bg=C["fondo"],
                         relief="solid", bd=1)
            e.pack(side="left", padx=5)

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

        if agregar_producto(nombre, precio):
            self._cargar_productos()
            self.nuevo_nombre.set("")
            messagebox.showinfo("Listo", f"Producto '{nombre}' agregado.")
        else:
            messagebox.showwarning("Atencion", "Ese producto ya existe.")

    def _seccion_usuarios(self, parent):
        sec = _seccion(parent, "Usuarios")

        self.usuarios_frame = tk.Frame(sec, bg=C["tarjeta"])
        self.usuarios_frame.pack(fill="x", pady=5)

        self._cargar_usuarios()

        # Agregar usuario
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

        usuarios = obtener_usuarios()
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

        if agregar_usuario(nombre, pin, rol):
            self._cargar_usuarios()
            self.nuevo_user_nombre.set("")
            self.nuevo_user_pin.set("")
            messagebox.showinfo("Listo", f"Usuario '{nombre}' agregado como {rol}.")
        else:
            messagebox.showerror("Error", "No se pudo agregar el usuario.")

    def _eliminar_usuario(self, uid):
        if messagebox.askyesno("Confirmar",
                                "Seguro que quieres eliminar este usuario?"):
            if eliminar_usuario(uid):
                self._cargar_usuarios()

    def _seccion_info(self, parent):
        sec = _seccion(parent, "Acerca del Sistema")

        info = [
            ("Version",  "2.0.0"),
            ("Motor",    "Python + SQLite"),
            ("Modelos",  "Estimacion -> Promedio Semanal -> Por Dia"),
        ]
        for k, v in info:
            fila = tk.Frame(sec, bg=C["tarjeta"])
            fila.pack(fill="x", pady=3)
            tk.Label(fila, text=f"{k}:", font=F_NORMAL,
                     bg=C["tarjeta"], fg=C["texto_suave"],
                     width=15, anchor="w").pack(side="left")
            tk.Label(fila, text=v, font=F_NORMAL_B,
                     bg=C["tarjeta"], fg=C["primario"]).pack(side="left")


# ══════════════════════════════════════════════
# WIDGETS REUTILIZABLES
# ══════════════════════════════════════════════

def _tarjeta(parent, titulo: str, valor: str, color: str):
    """Tarjeta de metrica grande."""
    card = tk.Frame(parent, bg=C["tarjeta"], padx=25, pady=15,
                     bd=1, relief="solid")
    card.pack(side="left", padx=8, fill="y")

    tk.Label(card, text=titulo, font=F_NORMAL,
             bg=C["tarjeta"], fg=C["texto_suave"]).pack()
    tk.Label(card, text=valor, font=("Arial", 28, "bold"),
             bg=C["tarjeta"], fg=color).pack(pady=(5, 0))


def _seccion(parent, titulo: str) -> tk.Frame:
    """Seccion con titulo y contenedor."""
    tk.Label(parent, text=titulo, font=F_SUBTIT,
             bg=C["fondo"], fg=C["secundario"],
             padx=25, pady=(10), anchor="w").pack(fill="x")

    contenedor = tk.Frame(parent, bg=C["tarjeta"], padx=20, pady=15,
                           bd=1, relief="solid")
    contenedor.pack(fill="x", padx=25, pady=(0, 15))
    return contenedor


def _campo_label(parent, texto: str, fila: int):
    """Label de campo de formulario."""
    tk.Label(parent, text=texto, font=F_NORMAL,
             bg=C["tarjeta"], fg=C["texto_suave"],
             anchor="w").grid(row=fila, column=0, sticky="w",
                               pady=10, padx=(0, 15))


def _campo_entry(parent, var: tk.StringVar, fila: int) -> tk.Entry:
    """Entry de formulario con estilo."""
    e = tk.Entry(parent, textvariable=var, font=F_GRANDE,
                 bg=C["fondo"], fg=C["texto"],
                 insertbackground=C["primario"],
                 relief="solid", bd=1, width=28,
                 highlightthickness=2,
                 highlightbackground=C["borde"],
                 highlightcolor=C["primario"])
    e.grid(row=fila, column=1, sticky="ew", pady=10)
    return e


# ──────────────────────────────────────────────
# Punto de entrada
# ──────────────────────────────────────────────

if __name__ == "__main__":
    app = PanaderiaApp()
    app.mainloop()
