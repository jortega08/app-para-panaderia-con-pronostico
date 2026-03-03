"""
app.py
------
Interfaz gráfica principal de la aplicación Panadería Lean.
Framework: Tkinter (incluido en Python estándar) + Matplotlib para gráficas.

Vistas:
  1. Dashboard    — métricas del día y pronóstico
  2. Registrar    — ingreso de producción y ventas
  3. Historial    — tabla de registros recientes
  4. Gráficas     — visualización de tendencias
  5. Configuración — productos y ajustes
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from datetime import datetime, timedelta
from typing import Optional
import sys
import os

# Asegurar que el path del proyecto esté disponible
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.database import (
    inicializar_base_de_datos,
    guardar_registro,
    obtener_registros,
    obtener_productos,
    agregar_producto,
)
from logic.pronostico import (
    calcular_pronostico,
    calcular_eficiencia,
    analizar_tendencia,
)

# ──────────────────────────────────────────────
# Paleta de colores y estilos (Tema panadería)
# ──────────────────────────────────────────────
COLORES = {
    "fondo":        "#1C1A16",   # marrón oscuro profundo
    "fondo_panel":  "#2A2520",   # panel secundario
    "fondo_card":   "#332E28",   # tarjeta
    "acento":       "#E8A030",   # dorado pan
    "acento2":      "#C65D1E",   # naranja horneado
    "texto":        "#F5EFDF",   # crema
    "texto_suave":  "#A89880",   # texto secundario
    "verde":        "#4CAF7D",   # estado ok
    "amarillo":     "#F0C040",   # advertencia
    "rojo":         "#E05050",   # peligro
    "borde":        "#4A3F35",   # bordes
}

FUENTE_TITULO  = ("Georgia", 22, "bold")
FUENTE_SUBTIT  = ("Georgia", 14, "bold")
FUENTE_NORMAL  = ("Courier New", 11)
FUENTE_GRANDE  = ("Courier New", 16, "bold")
FUENTE_PEQUEÑA = ("Courier New", 9)


class PanaderiaApp(tk.Tk):
    """Ventana principal de la aplicación. Administra la navegación entre vistas."""

    def __init__(self):
        super().__init__()
        self.title("🍞 Panadería Lean — Sistema de Pronóstico")
        self.geometry("1100x700")
        self.minsize(900, 600)
        self.configure(bg=COLORES["fondo"])

        inicializar_base_de_datos()
        self._configurar_estilos()
        self._construir_layout()
        self._mostrar_vista("dashboard")

    # ──────────────────────────────────────────
    # Configuración visual (ttk styles)
    # ──────────────────────────────────────────

    def _configurar_estilos(self):
        estilo = ttk.Style(self)
        estilo.theme_use("clam")

        estilo.configure("TFrame", background=COLORES["fondo"])
        estilo.configure("Card.TFrame", background=COLORES["fondo_card"],
                         relief="flat")
        estilo.configure(
            "Accent.TButton",
            background=COLORES["acento"],
            foreground=COLORES["fondo"],
            font=("Courier New", 11, "bold"),
            borderwidth=0,
            padding=(16, 8),
        )
        estilo.map("Accent.TButton",
                   background=[("active", COLORES["acento2"])])

        estilo.configure(
            "Nav.TButton",
            background=COLORES["fondo_panel"],
            foreground=COLORES["texto_suave"],
            font=("Courier New", 10),
            borderwidth=0,
            padding=(12, 10),
        )
        estilo.map("Nav.TButton",
                   background=[("active", COLORES["fondo_card"])],
                   foreground=[("active", COLORES["acento"])])

        estilo.configure(
            "Treeview",
            background=COLORES["fondo_card"],
            foreground=COLORES["texto"],
            fieldbackground=COLORES["fondo_card"],
            font=FUENTE_NORMAL,
            rowheight=28,
        )
        estilo.configure(
            "Treeview.Heading",
            background=COLORES["fondo_panel"],
            foreground=COLORES["acento"],
            font=("Courier New", 10, "bold"),
        )
        estilo.map("Treeview",
                   background=[("selected", COLORES["acento2"])],
                   foreground=[("selected", COLORES["texto"])])

        estilo.configure(
            "TCombobox",
            fieldbackground=COLORES["fondo_card"],
            background=COLORES["fondo_card"],
            foreground=COLORES["texto"],
            selectbackground=COLORES["acento"],
        )

        self.option_add("*TCombobox*Listbox.background", COLORES["fondo_card"])
        self.option_add("*TCombobox*Listbox.foreground", COLORES["texto"])

    # ──────────────────────────────────────────
    # Layout principal
    # ──────────────────────────────────────────

    def _construir_layout(self):
        # Barra lateral de navegación
        self.nav_frame = tk.Frame(self, bg=COLORES["fondo_panel"], width=200)
        self.nav_frame.pack(side="left", fill="y")
        self.nav_frame.pack_propagate(False)

        # Área de contenido principal
        self.content_frame = tk.Frame(self, bg=COLORES["fondo"])
        self.content_frame.pack(side="left", fill="both", expand=True)

        self._construir_nav()

    def _construir_nav(self):
        # Logo / título
        tk.Label(
            self.nav_frame,
            text="🍞",
            font=("", 36),
            bg=COLORES["fondo_panel"],
            fg=COLORES["acento"],
        ).pack(pady=(30, 4))

        tk.Label(
            self.nav_frame,
            text="Panadería\nLean",
            font=("Georgia", 13, "bold"),
            bg=COLORES["fondo_panel"],
            fg=COLORES["texto"],
            justify="center",
        ).pack()

        tk.Label(
            self.nav_frame,
            text="Sistema DMAIC",
            font=FUENTE_PEQUEÑA,
            bg=COLORES["fondo_panel"],
            fg=COLORES["texto_suave"],
        ).pack(pady=(0, 20))

        ttk.Separator(self.nav_frame, orient="horizontal").pack(
            fill="x", padx=20, pady=10
        )

        # Botones de navegación
        nav_items = [
            ("📊  Dashboard",      "dashboard"),
            ("✏️  Registrar",      "registrar"),
            ("📋  Historial",      "historial"),
            ("📈  Gráficas",       "graficas"),
            ("⚙️  Configuración",  "configuracion"),
        ]
        self.nav_buttons: dict[str, ttk.Button] = {}
        for texto, vista in nav_items:
            btn = ttk.Button(
                self.nav_frame,
                text=texto,
                style="Nav.TButton",
                command=lambda v=vista: self._mostrar_vista(v),
            )
            btn.pack(fill="x", padx=10, pady=2)
            self.nav_buttons[vista] = btn

        # Fecha en la parte inferior de nav
        self.nav_frame.pack_propagate(False)
        tk.Label(
            self.nav_frame,
            textvariable=self._reloj_var(),
            font=FUENTE_PEQUEÑA,
            bg=COLORES["fondo_panel"],
            fg=COLORES["texto_suave"],
            justify="center",
        ).pack(side="bottom", pady=20)

    def _reloj_var(self) -> tk.StringVar:
        var = tk.StringVar()
        def actualizar():
            ahora = datetime.now()
            var.set(ahora.strftime("%d/%m/%Y\n%H:%M:%S"))
            self.after(1000, actualizar)
        actualizar()
        return var

    # ──────────────────────────────────────────
    # Navegación entre vistas
    # ──────────────────────────────────────────

    def _mostrar_vista(self, nombre: str):
        # Limpiar contenido actual
        for widget in self.content_frame.winfo_children():
            widget.destroy()

        # Resaltar botón activo
        for vista, btn in self.nav_buttons.items():
            btn.configure(style="Nav.TButton")

        vistas = {
            "dashboard":    VistaDashboard,
            "registrar":    VistaRegistrar,
            "historial":    VistaHistorial,
            "graficas":     VistaGraficas,
            "configuracion": VistaConfiguracion,
        }
        if nombre in vistas:
            vistas[nombre](self.content_frame, self).pack(fill="both", expand=True)


# ──────────────────────────────────────────────────────────────────────────────
# VISTA 1: Dashboard
# ──────────────────────────────────────────────────────────────────────────────

class VistaDashboard(ttk.Frame):
    """Panel principal con métricas del día y pronóstico por producto."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent)
        self.configure(style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        # Encabezado
        header = tk.Frame(self, bg=COLORES["fondo"], pady=20, padx=30)
        header.pack(fill="x")

        tk.Label(
            header,
            text="Dashboard de Producción",
            font=FUENTE_TITULO,
            bg=COLORES["fondo"],
            fg=COLORES["texto"],
        ).pack(side="left")

        fecha_hoy = datetime.now().strftime("%A %d de %B, %Y")
        tk.Label(
            header,
            text=fecha_hoy,
            font=FUENTE_NORMAL,
            bg=COLORES["fondo"],
            fg=COLORES["texto_suave"],
        ).pack(side="right", padx=10)

        # Selector de producto
        control = tk.Frame(self, bg=COLORES["fondo"], padx=30)
        control.pack(fill="x")

        tk.Label(
            control, text="Producto:", font=FUENTE_NORMAL,
            bg=COLORES["fondo"], fg=COLORES["texto_suave"]
        ).pack(side="left")

        self.producto_var = tk.StringVar()
        productos = obtener_productos()
        self.combo = ttk.Combobox(
            control, textvariable=self.producto_var,
            values=productos, state="readonly", width=25,
            font=FUENTE_NORMAL
        )
        if productos:
            self.combo.current(0)
        self.combo.pack(side="left", padx=10)
        self.combo.bind("<<ComboboxSelected>>", lambda _: self._actualizar())

        ttk.Button(
            control, text="↺ Actualizar",
            style="Accent.TButton",
            command=self._actualizar
        ).pack(side="left", padx=5)

        # Área de métricas
        self.metrics_frame = tk.Frame(self, bg=COLORES["fondo"])
        self.metrics_frame.pack(fill="both", expand=True, padx=30, pady=20)

        self._actualizar()

    def _actualizar(self):
        for w in self.metrics_frame.winfo_children():
            w.destroy()

        producto = self.producto_var.get()
        if not producto:
            tk.Label(
                self.metrics_frame, text="Selecciona un producto",
                bg=COLORES["fondo"], fg=COLORES["texto_suave"],
                font=FUENTE_SUBTIT
            ).pack(pady=50)
            return

        resultado = calcular_pronostico(producto)
        registros = obtener_registros(producto, dias=7)
        eficiencia = calcular_eficiencia(registros)
        tendencia = analizar_tendencia(registros)

        # ── Fila de tarjetas métricas
        tarjetas_frame = tk.Frame(self.metrics_frame, bg=COLORES["fondo"])
        tarjetas_frame.pack(fill="x", pady=(0, 20))

        metricas = [
            ("🎯 Producción\nSugerida",
             f"{resultado.produccion_sugerida} uds",
             COLORES["acento"]),
            ("📊 Nivel\nSigma",
             f"{resultado.nivel_sigma:.1f}σ",
             _color_sigma(resultado.nivel_sigma)),
            ("📉 Aprovechamiento",
             f"{eficiencia.get('tasa_aprovechamiento', 0)}%",
             COLORES["verde"]),
            ("📅 Historial",
             f"{resultado.dias_historial} días",
             COLORES["texto_suave"]),
            ("📈 Tendencia",
             tendencia.capitalize(),
             COLORES["amarillo"]),
        ]

        for titulo, valor, color in metricas:
            _tarjeta_metrica(tarjetas_frame, titulo, valor, color)

        # ── Banner de estado
        color_estado = {
            "optimal": COLORES["verde"],
            "warning": COLORES["amarillo"],
            "danger":  COLORES["rojo"],
        }.get(resultado.estado, COLORES["texto_suave"])

        banner = tk.Frame(
            self.metrics_frame,
            bg=color_estado, pady=12, padx=20,
        )
        banner.pack(fill="x", pady=(0, 20))
        tk.Label(
            banner,
            text=resultado.mensaje_estado,
            font=("Courier New", 12, "bold"),
            bg=color_estado,
            fg=COLORES["fondo"],
        ).pack()

        # ── Detalle del modelo
        info_frame = tk.Frame(
            self.metrics_frame, bg=COLORES["fondo_card"],
            padx=20, pady=15
        )
        info_frame.pack(fill="x")

        detalles = [
            ("Modelo activo",    resultado.modelo_usado.replace("_", " ").title()),
            ("Confianza",        resultado.confianza.upper()),
            ("Promedio ventas",  f"{resultado.promedio_ventas} uds/día"),
            ("Buffer aplicado",  "10% sobre promedio"),
            ("DMAIC - Fase",     "Medir → Controlar" if resultado.dias_historial < 30
                                 else "Controlar → Mejorar"),
        ]
        for label, valor in detalles:
            fila = tk.Frame(info_frame, bg=COLORES["fondo_card"])
            fila.pack(fill="x", pady=2)
            tk.Label(fila, text=label + ":", font=FUENTE_NORMAL,
                     bg=COLORES["fondo_card"], fg=COLORES["texto_suave"],
                     width=22, anchor="w").pack(side="left")
            tk.Label(fila, text=valor, font=("Courier New", 11, "bold"),
                     bg=COLORES["fondo_card"], fg=COLORES["texto"]).pack(side="left")


# ──────────────────────────────────────────────────────────────────────────────
# VISTA 2: Registrar
# ──────────────────────────────────────────────────────────────────────────────

class VistaRegistrar(ttk.Frame):
    """Formulario de ingreso diario de producción y ventas."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent)
        self.configure(style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        # Título
        tk.Label(
            self, text="Registrar Producción del Día",
            font=FUENTE_TITULO, bg=COLORES["fondo"], fg=COLORES["texto"],
            pady=20, padx=30
        ).pack(fill="x")

        # Formulario centrado
        form_outer = tk.Frame(self, bg=COLORES["fondo"])
        form_outer.pack(expand=True)

        form = tk.Frame(
            form_outer, bg=COLORES["fondo_card"],
            padx=40, pady=30
        )
        form.pack(padx=20)

        # ── Campos del formulario
        campos = {}

        def campo(parent, etiqueta: str, fila: int,
                  tipo: str = "entry", opciones=None) -> tk.Variable:
            tk.Label(
                parent, text=etiqueta, font=FUENTE_NORMAL,
                bg=COLORES["fondo_card"], fg=COLORES["texto_suave"],
                anchor="w"
            ).grid(row=fila, column=0, sticky="w", pady=8, padx=(0, 20))

            if tipo == "combo":
                var = tk.StringVar()
                widget = ttk.Combobox(
                    parent, textvariable=var, values=opciones,
                    state="readonly", font=FUENTE_NORMAL, width=28
                )
                if opciones:
                    widget.current(0)
            else:
                var = tk.StringVar()
                widget = tk.Entry(
                    parent, textvariable=var, font=FUENTE_NORMAL,
                    bg=COLORES["fondo"], fg=COLORES["texto"],
                    insertbackground=COLORES["acento"],
                    relief="flat", bd=0, width=30,
                    highlightthickness=1,
                    highlightbackground=COLORES["borde"],
                    highlightcolor=COLORES["acento"]
                )

            widget.grid(row=fila, column=1, sticky="ew", pady=8)
            return var

        # Fecha (por defecto hoy)
        self.fecha_var = campo(form, "📅  Fecha (YYYY-MM-DD)", 0)
        self.fecha_var.set(datetime.now().strftime("%Y-%m-%d"))

        # Producto
        self.producto_var = campo(form, "🥖  Producto", 1, "combo", obtener_productos())

        # Producido
        self.producido_var = campo(form, "🏭  Cantidad producida", 2)

        # Vendido
        self.vendido_var = campo(form, "💰  Cantidad vendida", 3)

        # Observaciones
        self.obs_var = campo(form, "📝  Observaciones (opcional)", 4)

        # Indicador de sobrante (calculado)
        tk.Label(form, text="📦  Sobrante estimado:", font=FUENTE_NORMAL,
                 bg=COLORES["fondo_card"], fg=COLORES["texto_suave"],
                 anchor="w").grid(row=5, column=0, sticky="w", pady=8)

        self.sobrante_label = tk.Label(
            form, text="—", font=("Courier New", 13, "bold"),
            bg=COLORES["fondo_card"], fg=COLORES["acento"]
        )
        self.sobrante_label.grid(row=5, column=1, sticky="w", pady=8)

        # Actualizar sobrante en tiempo real
        for var in (self.producido_var, self.vendido_var):
            var.trace_add("write", self._actualizar_sobrante)

        # Botón guardar
        ttk.Button(
            form, text="✅  Guardar Registro",
            style="Accent.TButton",
            command=self._guardar
        ).grid(row=6, column=0, columnspan=2, pady=20, ipadx=20)

    def _actualizar_sobrante(self, *_):
        try:
            prod = int(self.producido_var.get())
            vend = int(self.vendido_var.get())
            sobrante = prod - vend
            color = COLORES["verde"] if sobrante >= 0 else COLORES["rojo"]
            self.sobrante_label.configure(
                text=f"{sobrante} unidades", fg=color
            )
        except ValueError:
            self.sobrante_label.configure(text="—", fg=COLORES["acento"])

    def _guardar(self):
        try:
            fecha    = self.fecha_var.get().strip()
            producto = self.producto_var.get()
            producido = int(self.producido_var.get())
            vendido   = int(self.vendido_var.get())
            obs       = self.obs_var.get().strip()

            # Validaciones
            if not producto:
                messagebox.showwarning("Atención", "Selecciona un producto.")
                return
            if producido < 0 or vendido < 0:
                messagebox.showwarning("Atención", "Los valores no pueden ser negativos.")
                return
            if vendido > producido:
                messagebox.showwarning(
                    "Atención",
                    "Vendido no puede ser mayor a producido.\n"
                    "¿Hubo producción extra no registrada?"
                )
                return
            datetime.strptime(fecha, "%Y-%m-%d")  # validar formato

        except ValueError as e:
            messagebox.showerror("Error", f"Datos inválidos: {e}")
            return

        exito = guardar_registro(fecha, producto, producido, vendido, obs)
        if exito:
            messagebox.showinfo(
                "✅ Guardado",
                f"Registro guardado:\n{producto} — {fecha}\n"
                f"Producido: {producido}  |  Vendido: {vendido}  |  Sobrante: {producido - vendido}"
            )
            # Limpiar campos numéricos
            self.producido_var.set("")
            self.vendido_var.set("")
            self.obs_var.set("")
        else:
            messagebox.showerror("Error", "No se pudo guardar el registro.")


# ──────────────────────────────────────────────────────────────────────────────
# VISTA 3: Historial
# ──────────────────────────────────────────────────────────────────────────────

class VistaHistorial(ttk.Frame):
    """Tabla de registros históricos con filtros."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent)
        self.configure(style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        tk.Label(
            self, text="Historial de Registros",
            font=FUENTE_TITULO, bg=COLORES["fondo"], fg=COLORES["texto"],
            pady=20, padx=30
        ).pack(fill="x")

        # Controles de filtro
        filtros = tk.Frame(self, bg=COLORES["fondo"], padx=30)
        filtros.pack(fill="x", pady=(0, 10))

        tk.Label(filtros, text="Producto:", font=FUENTE_NORMAL,
                 bg=COLORES["fondo"], fg=COLORES["texto_suave"]).pack(side="left")

        self.filtro_producto = tk.StringVar(value="Todos")
        combo = ttk.Combobox(
            filtros, textvariable=self.filtro_producto,
            values=["Todos"] + obtener_productos(),
            state="readonly", font=FUENTE_NORMAL, width=20
        )
        combo.pack(side="left", padx=10)
        combo.bind("<<ComboboxSelected>>", lambda _: self._cargar())

        tk.Label(filtros, text="Últimos:", font=FUENTE_NORMAL,
                 bg=COLORES["fondo"], fg=COLORES["texto_suave"]).pack(side="left", padx=(20, 5))

        self.dias_var = tk.IntVar(value=30)
        for dias in (7, 14, 30, 60, 90):
            tk.Radiobutton(
                filtros, text=f"{dias}d", variable=self.dias_var, value=dias,
                command=self._cargar,
                bg=COLORES["fondo"], fg=COLORES["texto_suave"],
                selectcolor=COLORES["fondo_card"],
                activebackground=COLORES["fondo"],
                font=FUENTE_PEQUEÑA
            ).pack(side="left", padx=4)

        # Tabla
        tabla_frame = tk.Frame(self, bg=COLORES["fondo"], padx=30)
        tabla_frame.pack(fill="both", expand=True, pady=10)

        columnas = ("fecha", "dia", "producto", "producido", "vendido",
                    "sobrante", "observaciones")
        self.tabla = ttk.Treeview(
            tabla_frame, columns=columnas, show="headings", height=18
        )

        encabezados = {
            "fecha": ("Fecha", 100),
            "dia": ("Día", 90),
            "producto": ("Producto", 140),
            "producido": ("Producido", 90),
            "vendido": ("Vendido", 80),
            "sobrante": ("Sobrante", 80),
            "observaciones": ("Notas", 200),
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
            producto if producto != "Todos" else None,
            dias=dias
        )

        for r in registros:
            sobrante = r["sobrante"]
            tag = "normal"
            if sobrante < 0:
                tag = "negativo"
            elif sobrante / max(r["producido"], 1) > 0.15:
                tag = "alto_sobrante"

            self.tabla.insert("", "end", values=(
                r["fecha"], r["dia_semana"], r["producto"],
                r["producido"], r["vendido"], sobrante,
                r["observaciones"]
            ), tags=(tag,))

        self.tabla.tag_configure("alto_sobrante",
                                 background="#3D2A1A", foreground=COLORES["amarillo"])
        self.tabla.tag_configure("negativo",
                                 background="#3D1A1A", foreground=COLORES["rojo"])


# ──────────────────────────────────────────────────────────────────────────────
# VISTA 4: Gráficas
# ──────────────────────────────────────────────────────────────────────────────

class VistaGraficas(ttk.Frame):
    """Visualizaciones de tendencias usando Matplotlib embebido."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent)
        self.configure(style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        try:
            import matplotlib
            matplotlib.use("TkAgg")
            from matplotlib.figure import Figure
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
            self._Figure = Figure
            self._FigureCanvasTkAgg = FigureCanvasTkAgg
            self._matplotlib_disponible = True
        except ImportError:
            self._matplotlib_disponible = False

        tk.Label(
            self, text="Gráficas de Producción",
            font=FUENTE_TITULO, bg=COLORES["fondo"], fg=COLORES["texto"],
            pady=20, padx=30
        ).pack(fill="x")

        # Controles
        ctrl = tk.Frame(self, bg=COLORES["fondo"], padx=30)
        ctrl.pack(fill="x", pady=(0, 10))

        tk.Label(ctrl, text="Producto:", font=FUENTE_NORMAL,
                 bg=COLORES["fondo"], fg=COLORES["texto_suave"]).pack(side="left")

        self.producto_var = tk.StringVar()
        productos = obtener_productos()
        combo = ttk.Combobox(ctrl, textvariable=self.producto_var,
                             values=productos, state="readonly",
                             font=FUENTE_NORMAL, width=22)
        if productos:
            combo.current(0)
        combo.pack(side="left", padx=10)
        combo.bind("<<ComboboxSelected>>", lambda _: self._graficar())

        ttk.Button(ctrl, text="📈 Generar Gráfica",
                   style="Accent.TButton",
                   command=self._graficar).pack(side="left", padx=5)

        self.canvas_frame = tk.Frame(self, bg=COLORES["fondo"])
        self.canvas_frame.pack(fill="both", expand=True, padx=30, pady=10)

        if not self._matplotlib_disponible:
            tk.Label(
                self.canvas_frame,
                text="⚠️  Para ver gráficas instala matplotlib:\n\npip install matplotlib",
                font=FUENTE_NORMAL,
                bg=COLORES["fondo"],
                fg=COLORES["amarillo"],
                justify="center"
            ).pack(expand=True)
        else:
            self._graficar()

    def _graficar(self):
        if not self._matplotlib_disponible:
            return

        for w in self.canvas_frame.winfo_children():
            w.destroy()

        producto = self.producto_var.get()
        if not producto:
            return

        registros = obtener_registros(producto, dias=30)
        if not registros:
            tk.Label(
                self.canvas_frame,
                text="Sin datos para graficar.\nRegistra al menos un día.",
                font=FUENTE_NORMAL, bg=COLORES["fondo"], fg=COLORES["texto_suave"]
            ).pack(expand=True)
            return

        # Preparar datos
        registros_ord = list(reversed(registros))
        fechas    = [r["fecha"][-5:] for r in registros_ord]  # MM-DD
        producido = [r["producido"] for r in registros_ord]
        vendido   = [r["vendido"]   for r in registros_ord]
        sobrante  = [r["sobrante"]  for r in registros_ord]

        # Crear figura
        fig = self._Figure(figsize=(10, 5.5), facecolor=COLORES["fondo"])
        fig.subplots_adjust(hspace=0.4)

        ax1 = fig.add_subplot(211)
        ax2 = fig.add_subplot(212)

        # ── Gráfica 1: Producido vs Vendido
        x = range(len(fechas))
        ax1.bar([i - 0.2 for i in x], producido, width=0.4,
                label="Producido", color=COLORES["acento2"], alpha=0.85)
        ax1.bar([i + 0.2 for i in x], vendido, width=0.4,
                label="Vendido", color=COLORES["acento"], alpha=0.85)
        ax1.plot(x, vendido, color=COLORES["texto"], linewidth=1.5,
                 linestyle="--", alpha=0.6)

        ax1.set_title(f"Producido vs Vendido — {producto}",
                      color=COLORES["texto"], fontsize=10, pad=10)
        ax1.set_xticks(list(x))
        ax1.set_xticklabels(fechas, rotation=45, fontsize=7,
                            color=COLORES["texto_suave"])
        ax1.tick_params(colors=COLORES["texto_suave"])
        ax1.set_facecolor(COLORES["fondo_card"])
        ax1.legend(fontsize=8, labelcolor=COLORES["texto"],
                   facecolor=COLORES["fondo_panel"])
        for spine in ax1.spines.values():
            spine.set_color(COLORES["borde"])
        ax1.yaxis.label.set_color(COLORES["texto_suave"])
        ax1.tick_params(axis='y', colors=COLORES["texto_suave"])

        # ── Gráfica 2: Sobrante diario
        colores_sob = [COLORES["rojo"] if s > max(producido[i] * 0.15, 1)
                       else COLORES["verde"] for i, s in enumerate(sobrante)]
        ax2.bar(x, sobrante, color=colores_sob, alpha=0.85)
        ax2.axhline(0, color=COLORES["texto_suave"], linewidth=0.8, linestyle="--")

        ax2.set_title("Sobrante por Día (🔴 > 15% = alerta)",
                      color=COLORES["texto"], fontsize=10, pad=10)
        ax2.set_xticks(list(x))
        ax2.set_xticklabels(fechas, rotation=45, fontsize=7,
                            color=COLORES["texto_suave"])
        ax2.set_facecolor(COLORES["fondo_card"])
        ax2.tick_params(colors=COLORES["texto_suave"])
        for spine in ax2.spines.values():
            spine.set_color(COLORES["borde"])
        ax2.tick_params(axis='y', colors=COLORES["texto_suave"])

        canvas = self._FigureCanvasTkAgg(fig, self.canvas_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)


# ──────────────────────────────────────────────────────────────────────────────
# VISTA 5: Configuración
# ──────────────────────────────────────────────────────────────────────────────

class VistaConfiguracion(ttk.Frame):
    """Gestión de productos y ajustes del sistema."""

    def __init__(self, parent, app: PanaderiaApp):
        super().__init__(parent)
        self.configure(style="TFrame")
        self.app = app
        self._construir()

    def _construir(self):
        tk.Label(
            self, text="Configuración",
            font=FUENTE_TITULO, bg=COLORES["fondo"], fg=COLORES["texto"],
            pady=20, padx=30
        ).pack(fill="x")

        # ── Gestión de productos
        sec = _seccion(self, "🥖 Gestión de Productos")

        lista_frame = tk.Frame(sec, bg=COLORES["fondo_card"])
        lista_frame.pack(fill="x", pady=10)

        self.listbox_productos = tk.Listbox(
            lista_frame, font=FUENTE_NORMAL,
            bg=COLORES["fondo"], fg=COLORES["texto"],
            selectbackground=COLORES["acento"],
            relief="flat", height=8, bd=0
        )
        self.listbox_productos.pack(side="left", fill="both", expand=True)

        self._cargar_productos()

        btn_frame = tk.Frame(lista_frame, bg=COLORES["fondo_card"])
        btn_frame.pack(side="right", padx=10, fill="y")

        ttk.Button(btn_frame, text="➕ Agregar",
                   style="Accent.TButton",
                   command=self._agregar_producto).pack(pady=5)

        # ── Info del sistema
        _seccion_info = _seccion(self, "ℹ️ Acerca del Sistema")
        info = [
            ("Metodología",  "Lean Six Sigma — DMAIC"),
            ("Motor",        "Python + SQLite"),
            ("Modelos",      "Base → Promedio Móvil → Por Día"),
            ("Versión",      "1.0.0"),
        ]
        for k, v in info:
            fila = tk.Frame(_seccion_info, bg=COLORES["fondo_card"])
            fila.pack(fill="x", pady=3)
            tk.Label(fila, text=f"{k}:", font=FUENTE_NORMAL,
                     bg=COLORES["fondo_card"], fg=COLORES["texto_suave"],
                     width=18, anchor="w").pack(side="left")
            tk.Label(fila, text=v, font=("Courier New", 11, "bold"),
                     bg=COLORES["fondo_card"],
                     fg=COLORES["acento"]).pack(side="left")

    def _cargar_productos(self):
        self.listbox_productos.delete(0, tk.END)
        for p in obtener_productos():
            self.listbox_productos.insert(tk.END, f"  {p}")

    def _agregar_producto(self):
        nombre = simpledialog.askstring(
            "Nuevo Producto",
            "Nombre del producto:",
            parent=self
        )
        if nombre and nombre.strip():
            if agregar_producto(nombre.strip()):
                self._cargar_productos()
                messagebox.showinfo("✅", f"Producto '{nombre}' agregado.")
            else:
                messagebox.showwarning("Atención", "Ese producto ya existe.")


# ──────────────────────────────────────────────
# Widgets reutilizables
# ──────────────────────────────────────────────

def _tarjeta_metrica(parent, titulo: str, valor: str, color: str) -> tk.Frame:
    """Crea una tarjeta de métrica visual con título y valor."""
    card = tk.Frame(parent, bg=COLORES["fondo_card"], padx=18, pady=14)
    card.pack(side="left", padx=8, fill="y")

    tk.Label(card, text=titulo, font=("Courier New", 9),
             bg=COLORES["fondo_card"], fg=COLORES["texto_suave"],
             justify="center").pack()
    tk.Label(card, text=valor, font=("Courier New", 18, "bold"),
             bg=COLORES["fondo_card"], fg=color).pack(pady=(6, 0))
    return card


def _seccion(parent, titulo: str) -> tk.Frame:
    """Crea una sección con encabezado."""
    tk.Label(
        parent, text=titulo, font=FUENTE_SUBTIT,
        bg=COLORES["fondo"], fg=COLORES["acento"],
        padx=30, pady=10, anchor="w"
    ).pack(fill="x")

    contenedor = tk.Frame(parent, bg=COLORES["fondo_card"], padx=20, pady=15)
    contenedor.pack(fill="x", padx=30, pady=(0, 20))
    return contenedor


def _color_sigma(sigma: float) -> str:
    if sigma >= 3.0:
        return COLORES["verde"]
    elif sigma >= 2.0:
        return COLORES["amarillo"]
    return COLORES["rojo"]


# ──────────────────────────────────────────────
# Punto de entrada
# ──────────────────────────────────────────────

if __name__ == "__main__":
    app = PanaderiaApp()
    app.mainloop()
