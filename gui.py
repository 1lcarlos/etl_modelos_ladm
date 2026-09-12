# gui.py
"""
Interfaz gráfica del ETL PostgreSQL - LADM-COL
Permite elegir el flujo de migración, verificarlo, simularlo y ejecutarlo.
"""

import copy
import glob
import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import ttk, filedialog, messagebox, scrolledtext

# Configurar codificación UTF-8 para Windows (con pythonw no hay consola)
if sys.platform == 'win32':
    import io
    if sys.stdout is not None:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    if sys.stderr is not None:
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Directorio base del proyecto
BASE_DIR = Path(__file__).resolve().parent

# Imports locales
sys.path.insert(0, str(BASE_DIR))
from src import __version__
from src.database_manager import DatabaseManager
from src.etl_processor import ETLProcessor
from src.flujos import cargar_flujos, identificar_flujo, verificar_flujo

CONFIG_FILE = BASE_DIR / 'etl_config.json'
FLUJOS_FILE = BASE_DIR / 'config' / 'flujos.json'
PERSONALIZADO = 'Personalizado (rutas manuales)'

DEFAULT_CONFIG = {
    'flujo': '',
    'origen': {'host': 'localhost', 'port': '5432', 'database': '', 'user': 'postgres', 'password': ''},
    'destino': {'host': 'localhost', 'port': '5432', 'database': '', 'user': 'postgres', 'password': ''},
    'schema': '',
    'paths': {
        'queries': str(BASE_DIR / 'sql' / 'queries'),
        'inserts': str(BASE_DIR / 'sql' / 'inserts'),
        'order_file': str(BASE_DIR / 'sql' / 'insert_order.txt'),
        'logs': str(BASE_DIR / 'logs'),
    },
    'options': {
        'batch_size': '1000',
        'pool_size': '5',
        'truncate_enabled': True,
        'truncate_cascade': True,
        'dry_run': False,  # True = simulación con ROLLBACK
        'schema_mode': 'single',
    },
    'schemas': [],
}

COLOR = {'ok': '#1e7b34', 'error': '#b3261e', 'warning': '#9a6700', 'info': '#1f4e79', 'muted': '#6b7280'}

# estado -> (texto, tag de color)
ESTADOS = {
    'ok': ('✔ OK', 'ok'),
    'error': ('✖ Error', 'error'),
    'omitido': ('– Omitido', 'muted'),
    'cancelado': ('■ Cancelado', 'warning'),
    'advertencia': ('⚠ Revisar', 'warning'),
    'faltante': ('✖ No existe', 'error'),
}

FASES = {
    'truncate': 'Vaciando tablas destino',
    'queries': 'Consultando origen',
    'temporales': 'Creando tablas temporales',
    'inserts': 'Cargando destino',
    'limpieza': 'Eliminando tablas temporales',
}

# Avance dentro de un esquema al iniciar cada fase (0 a 1)
AVANCE_FASE = {'truncate': 0.0, 'queries': 0.05, 'temporales': 0.35, 'inserts': 0.40, 'limpieza': 0.99}


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def abrir_en_sistema(ruta):
    """Abre un archivo o carpeta con la aplicación predeterminada del sistema"""
    if sys.platform == 'win32':
        os.startfile(ruta)
    elif sys.platform == 'darwin':
        subprocess.Popen(['open', ruta])
    else:
        subprocess.Popen(['xdg-open', ruta])


def formato_duracion(segundos):
    minutos, seg = divmod(int(segundos), 60)
    horas, minutos = divmod(minutos, 60)
    return f"{horas:02d}:{minutos:02d}:{seg:02d}"


def formato_tiempo(segundos):
    if segundos is None:
        return ''
    return f"{segundos:.1f} s" if segundos < 60 else formato_duracion(segundos)


def formato_miles(numero):
    return f"{numero:,}".replace(',', '.')


def formato_filas(filas):
    if filas is None:
        return ''
    return '—' if filas < 0 else formato_miles(filas)


def primera_linea(texto, largo=160):
    linea = (texto or '').strip().splitlines()[0] if (texto or '').strip() else ''
    return linea if len(linea) <= largo else linea[:largo - 1] + '…'


def resumen_conexion(c):
    if not c.get('database'):
        return '(sin configurar)'
    return f"{c.get('user') or '?'}@{c.get('host') or '?'}:{c.get('port') or '?'}/{c['database']}"


def config_conexion(c, timeout=None):
    """Convierte los valores del formulario al formato de DatabaseManager"""
    puerto = str(c.get('port', '')).strip()
    try:
        puerto = int(puerto)
    except ValueError:
        raise ValueError(f"El puerto '{puerto}' no es un número")
    config = {
        'host': str(c.get('host', '')).strip(),
        'port': puerto,
        'database': str(c.get('database', '')).strip(),
        'user': str(c.get('user', '')).strip(),
        'password': c.get('password', ''),
    }
    if timeout:
        config['connect_timeout'] = timeout
    return config


def _conectar(c):
    db = DatabaseManager(config_conexion(c, timeout=10), pool_size=1)
    if not db.create_connection_pool():
        raise ConnectionError(db.last_error or 'No se pudo conectar')
    return db


def probar_conexion(c):
    """Abre una conexión de prueba; devuelve la versión de PostgreSQL o lanza el error"""
    db = _conectar(c)
    try:
        filas = db.execute_query('SELECT version() AS version')
        return filas[0]['version'].split(',')[0]
    finally:
        db.close_pool()


def listar_esquemas(c):
    db = _conectar(c)
    try:
        return db.list_schemas()
    finally:
        db.close_pool()


def entero(valor, defecto):
    try:
        return int(str(valor).strip())
    except (TypeError, ValueError):
        return defecto


class GuiLogHandler(logging.Handler):
    """Envía los mensajes del logger 'ETL' al visor de la GUI"""

    def __init__(self, ui_queue):
        super().__init__(level=logging.INFO)
        self.ui_queue = ui_queue
        self.setFormatter(logging.Formatter('%(asctime)s  %(message)s', datefmt='%H:%M:%S'))

    def emit(self, record):
        try:
            mensaje = self.format(record)
            if record.levelno >= logging.ERROR:
                nivel = 'error'
            elif record.levelno >= logging.WARNING:
                nivel = 'warning'
            elif '✅' in mensaje or '🎉' in mensaje:
                nivel = 'ok'
            else:
                nivel = 'info'
            self.ui_queue.put(('log', (mensaje + '\n', nivel)))
        except Exception:
            self.handleError(record)


# ---------------------------------------------------------------------------
# Diálogos
# ---------------------------------------------------------------------------

class Dialogo(tk.Toplevel):
    """Ventana modal base"""

    def __init__(self, app, titulo, redimensionable=False):
        super().__init__(app.root)
        self.app = app
        self.title(titulo)
        self.transient(app.root)
        self.resizable(redimensionable, redimensionable)
        self.result = None
        self.protocol('WM_DELETE_WINDOW', self.cancelar)
        self.bind('<Escape>', lambda e: self.cancelar())
        self.body = ttk.Frame(self, padding=14)
        self.body.pack(fill=tk.BOTH, expand=True)

    def botones(self, texto_ok, comando_ok, texto_cancelar='Cancelar'):
        fila = ttk.Frame(self.body)
        fila.pack(side=tk.BOTTOM, fill=tk.X, pady=(12, 0))
        cancelar = ttk.Button(fila, text=texto_cancelar, command=self.cancelar)
        cancelar.pack(side=tk.RIGHT)
        ok = ttk.Button(fila, text=texto_ok, command=comando_ok, style='Accent.TButton')
        ok.pack(side=tk.RIGHT, padx=(0, 6))
        return ok, cancelar

    def mostrar(self):
        """Centra el diálogo, espera a que se cierre y devuelve self.result"""
        self.update_idletasks()
        padre = self.app.root
        x = padre.winfo_rootx() + (padre.winfo_width() - self.winfo_reqwidth()) // 2
        y = padre.winfo_rooty() + (padre.winfo_height() - self.winfo_reqheight()) // 3
        self.geometry(f'+{max(x, 0)}+{max(y, 0)}')
        self.grab_set()
        self.focus_set()
        self.wait_window()
        return self.result

    def cancelar(self):
        self.result = None
        self.destroy()


class ConexionDialogo(Dialogo):
    """Edita y prueba los datos de conexión de una base de datos"""

    CAMPOS = [('Host', 'host'), ('Puerto', 'port'), ('Base de datos', 'database'),
              ('Usuario', 'user'), ('Contraseña', 'password')]

    def __init__(self, app, tipo, valores):
        super().__init__(app, f'Conexión {tipo}')
        self.vars = {clave: tk.StringVar(value=str(valores.get(clave, '') or '')) for _, clave in self.CAMPOS}
        self.probado = None  # (valores, resultado) de la última prueba

        ttk.Label(self.body, text=f'Base de datos {tipo}', style='Title.TLabel').pack(anchor='w', pady=(0, 8))
        form = ttk.Frame(self.body)
        form.pack(fill=tk.X)
        form.columnconfigure(1, weight=1)
        for fila, (etiqueta, clave) in enumerate(self.CAMPOS):
            ttk.Label(form, text=etiqueta + ':').grid(row=fila, column=0, sticky='e', padx=(0, 8), pady=3)
            entry = ttk.Entry(form, textvariable=self.vars[clave], width=38)
            entry.grid(row=fila, column=1, sticky='we', pady=3)
            if clave == 'password':
                entry.config(show='•')
                self.password_entry = entry
            if fila == 0:
                entry.focus_set()
        self.ver_password = tk.BooleanVar(value=False)
        ttk.Checkbutton(form, text='Mostrar contraseña', variable=self.ver_password,
                        command=self._alternar_password).grid(row=len(self.CAMPOS), column=1, sticky='w')

        prueba = ttk.Frame(self.body)
        prueba.pack(fill=tk.X, pady=(10, 0))
        self.btn_probar = ttk.Button(prueba, text='Probar conexión', command=self.probar)
        self.btn_probar.pack(side=tk.LEFT)
        self.lbl_prueba = ttk.Label(prueba, text='', wraplength=300)
        self.lbl_prueba.pack(side=tk.LEFT, padx=8)

        self.botones('Aceptar', self.aceptar)
        self.bind('<Return>', lambda e: self.aceptar())

    def _alternar_password(self):
        self.password_entry.config(show='' if self.ver_password.get() else '•')

    def valores(self):
        return {clave: (var.get() if clave == 'password' else var.get().strip()) for clave, var in self.vars.items()}

    def probar(self):
        valores = self.valores()
        try:
            config_conexion(valores)
        except ValueError as e:
            self.lbl_prueba.config(text=f'✖ {e}', foreground=COLOR['error'])
            return
        self.btn_probar.state(['disabled'])
        self.lbl_prueba.config(text='Conectando…', foreground=COLOR['muted'])

        def terminado(version, error):
            if not self.winfo_exists():
                return
            self.btn_probar.state(['!disabled'])
            if error:
                self.probado = (valores, False)
                self.lbl_prueba.config(text=f'✖ {primera_linea(str(error))}', foreground=COLOR['error'])
            else:
                self.probado = (valores, True)
                self.lbl_prueba.config(text=f'✔ {version}', foreground=COLOR['ok'])

        self.app.run_in_background(lambda: probar_conexion(valores), terminado)

    def aceptar(self):
        valores = self.valores()
        try:
            config_conexion(valores)
        except ValueError as e:
            messagebox.showerror('Dato inválido', str(e), parent=self)
            return
        estado = self.probado[1] if self.probado and self.probado[0] == valores else None
        self.result = (valores, estado)
        self.destroy()


class EsquemasDialogo(Dialogo):
    """Selección de varios esquemas de la BD origen"""

    def __init__(self, app, seleccionados):
        super().__init__(app, 'Seleccionar esquemas', redimensionable=True)
        self.todos = list(seleccionados)
        self.marcados = set(seleccionados)

        ttk.Label(self.body, text='Esquemas a migrar', style='Title.TLabel').pack(anchor='w')
        ttk.Label(self.body, text='Se consultan en la BD origen. Haga clic en un esquema para marcarlo o desmarcarlo.',
                  foreground=COLOR['muted']).pack(anchor='w', pady=(0, 6))

        barra = ttk.Frame(self.body)
        barra.pack(fill=tk.X)
        ttk.Label(barra, text='Filtrar:').pack(side=tk.LEFT)
        self.filtro = tk.StringVar()
        self.filtro.trace_add('write', lambda *args: self.refrescar())
        ttk.Entry(barra, textvariable=self.filtro, width=18).pack(side=tk.LEFT, padx=4)
        ttk.Button(barra, text='Marcar visibles', command=lambda: self.marcar_visibles(True)).pack(side=tk.LEFT, padx=2)
        ttk.Button(barra, text='Desmarcar visibles', command=lambda: self.marcar_visibles(False)).pack(side=tk.LEFT, padx=2)
        ttk.Button(barra, text='Volver a consultar', command=self.consultar).pack(side=tk.RIGHT)

        self.botones('Aceptar', self.aceptar)
        self.lbl_estado = ttk.Label(self.body, text='')
        self.lbl_estado.pack(side=tk.BOTTOM, anchor='w')

        marco = ttk.Frame(self.body)
        marco.pack(fill=tk.BOTH, expand=True, pady=6)
        self.tree = ttk.Treeview(marco, show='tree', selectmode='none', height=14)
        self.tree.column('#0', width=320)
        barra_v = ttk.Scrollbar(marco, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=barra_v.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        barra_v.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.bind('<ButtonRelease-1>', self.alternar)

        self.refrescar()
        self.consultar()

    def consultar(self):
        self.lbl_estado.config(text='Consultando esquemas en la BD origen…', foreground=COLOR['muted'])

        def terminado(esquemas, error):
            if not self.winfo_exists():
                return
            if error:
                self.lbl_estado.config(text=f'✖ No se pudo consultar: {primera_linea(str(error))}',
                                       foreground=COLOR['error'])
                return
            self.todos = sorted(set(esquemas) | self.marcados)
            self.refrescar()

        origen = dict(self.app.conexiones['origen'])
        self.app.run_in_background(lambda: listar_esquemas(origen), terminado)

    def _texto(self, esquema):
        return ('☑  ' if esquema in self.marcados else '☐  ') + esquema

    def _actualizar_estado(self):
        self.lbl_estado.config(text=f'{len(self.marcados)} seleccionado(s) de {len(self.todos)}',
                               foreground=COLOR['muted'])

    def refrescar(self):
        texto = self.filtro.get().strip().lower()
        self.tree.delete(*self.tree.get_children())
        for esquema in self.todos:
            if texto and texto not in esquema.lower():
                continue
            self.tree.insert('', tk.END, iid=esquema, text=self._texto(esquema))
        self._actualizar_estado()

    def alternar(self, event):
        item = self.tree.identify_row(event.y)
        if not item:
            return
        if item in self.marcados:
            self.marcados.discard(item)
        else:
            self.marcados.add(item)
        self.tree.item(item, text=self._texto(item))
        self._actualizar_estado()

    def marcar_visibles(self, marcar):
        for item in self.tree.get_children():
            if marcar:
                self.marcados.add(item)
            else:
                self.marcados.discard(item)
        self.refrescar()

    def aceptar(self):
        self.result = [e for e in self.todos if e in self.marcados]
        self.destroy()


class RutasDialogo(Dialogo):
    """Rutas de queries, inserts, archivo de orden y logs"""

    CAMPOS = [('Carpeta de queries', 'queries', 'carpeta'),
              ('Carpeta de inserts', 'inserts', 'carpeta'),
              ('Archivo de orden', 'order_file', 'archivo'),
              ('Carpeta de logs', 'logs', 'carpeta')]

    def __init__(self, app, rutas):
        super().__init__(app, 'Rutas del flujo')
        self.vars = {clave: tk.StringVar(value=rutas.get(clave, '')) for _, clave, _ in self.CAMPOS}

        ttk.Label(self.body, text='Rutas SQL', style='Title.TLabel').pack(anchor='w', pady=(0, 8))
        form = ttk.Frame(self.body)
        form.pack(fill=tk.X)
        form.columnconfigure(1, weight=1)
        for fila, (etiqueta, clave, tipo) in enumerate(self.CAMPOS):
            ttk.Label(form, text=etiqueta + ':').grid(row=fila, column=0, sticky='e', padx=(0, 8), pady=3)
            ttk.Entry(form, textvariable=self.vars[clave], width=70).grid(row=fila, column=1, sticky='we', pady=3)
            ttk.Button(form, text='…', width=3,
                       command=lambda c=clave, t=tipo: self.examinar(c, t)).grid(row=fila, column=2, padx=(4, 0))

        ayuda = ("• Queries: SELECT que se ejecutan en el origen; cada archivo crea la tabla tmp_<archivo> en el destino.\n"
                 "• Inserts: INSERT que cargan el destino a partir de las tablas tmp_.\n"
                 "• Orden: un archivo por línea; '#' es comentario y '-' desactiva el archivo. "
                 "Vacío = orden alfabético.")
        ttk.Label(self.body, text=ayuda, foreground=COLOR['muted'], justify=tk.LEFT).pack(anchor='w', pady=(8, 0))
        self.botones('Aceptar', self.aceptar)

    def examinar(self, clave, tipo):
        actual = self.vars[clave].get()
        if os.path.isdir(actual):
            inicial = actual
        elif os.path.isdir(os.path.dirname(actual)):
            inicial = os.path.dirname(actual)
        else:
            inicial = str(BASE_DIR)
        if tipo == 'carpeta':
            ruta = filedialog.askdirectory(parent=self, initialdir=inicial, title='Seleccionar carpeta')
        else:
            ruta = filedialog.askopenfilename(parent=self, initialdir=inicial, title='Seleccionar archivo de orden',
                                              filetypes=[('Archivos de texto', '*.txt'), ('Todos los archivos', '*.*')])
        if ruta:
            self.vars[clave].set(os.path.normpath(ruta))

    def aceptar(self):
        self.result = {clave: var.get().strip() for clave, var in self.vars.items()}
        self.destroy()


class ConfirmarDialogo(Dialogo):
    """Resumen de lo que se va a ejecutar, con la lista de tablas que se vaciarán"""

    def __init__(self, app, datos):
        simular = datos['simular']
        super().__init__(app, 'Confirmar simulación' if simular else 'Confirmar ejecución', redimensionable=True)

        titulo = '¿Simular la migración?' if simular else '¿Ejecutar la migración?'
        ttk.Label(self.body, text=titulo, style='Title.TLabel').pack(anchor='w')

        info = ttk.Frame(self.body)
        info.pack(fill=tk.X, pady=8)
        esquemas = datos['esquemas']
        if datos['truncate']:
            truncate = 'Sí, con CASCADE' if datos['cascade'] else 'Sí, sin CASCADE'
        else:
            truncate = 'No (los datos existentes se conservan; riesgo de duplicados)'
        filas = [
            ('Flujo', datos['flujo']),
            ('Origen', datos['origen']),
            ('Destino', datos['destino']),
            (f'Esquemas ({len(esquemas)})' if len(esquemas) > 1 else 'Esquema', ', '.join(esquemas)),
            ('Modo', 'Simulación: todo se deshace al final (ROLLBACK)' if simular else 'Real: los cambios se guardan'),
            ('Vaciar tablas', truncate),
        ]
        for fila, (clave, valor) in enumerate(filas):
            ttk.Label(info, text=clave + ':', style='Field.TLabel').grid(row=fila, column=0, sticky='nw', padx=(0, 10), pady=1)
            ttk.Label(info, text=valor, wraplength=480, justify=tk.LEFT).grid(row=fila, column=1, sticky='w', pady=1)

        aviso = ("No se guardará ningún cambio. Mientras dura la simulación las tablas destino quedan bloqueadas "
                 "y las secuencias (t_id) avanzan." if simular else
                 "Se modificarán datos en la base destino. Asegúrese de tener un backup.")
        ok, cancelar = self.botones('▶ Simular' if simular else '▶ Ejecutar', self.aceptar)
        ttk.Label(self.body, text=aviso, wraplength=560, justify=tk.LEFT,
                  foreground=COLOR['muted'] if simular else COLOR['error']).pack(side=tk.BOTTOM, anchor='w', pady=(8, 0))
        if datos['advertencias']:
            ttk.Label(self.body, text=f"⚠ La verificación encontró {datos['advertencias']} advertencia(s). "
                                      "Revise la pestaña Verificación.",
                      foreground=COLOR['warning']).pack(side=tk.BOTTOM, anchor='w', pady=(6, 0))

        tablas = datos['tablas'] if datos['truncate'] else []
        if tablas:
            sufijo = ' en cada esquema' if len(esquemas) > 1 else ''
            ttk.Label(self.body, text=f'Tablas que se vaciarán ({len(tablas)}){sufijo}:').pack(anchor='w')
            marco = ttk.Frame(self.body)
            marco.pack(fill=tk.BOTH, expand=True, pady=(2, 0))
            lista = tk.Listbox(marco, height=min(10, len(tablas)), activestyle='none', font=('Consolas', 9))
            barra_v = ttk.Scrollbar(marco, orient=tk.VERTICAL, command=lista.yview)
            lista.configure(yscrollcommand=barra_v.set)
            lista.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            barra_v.pack(side=tk.RIGHT, fill=tk.Y)
            for tabla in tablas:
                lista.insert(tk.END, tabla)

        # El foco queda en Cancelar para que Enter no ejecute por accidente
        cancelar.focus_set()

    def aceptar(self):
        self.result = True
        self.destroy()


class DetalleDialogo(Dialogo):
    """Texto completo de un resultado (por ejemplo, el error de un insert)"""

    def __init__(self, app, titulo, texto, ruta_sql=None):
        super().__init__(app, titulo, redimensionable=True)
        fila = ttk.Frame(self.body)
        fila.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 0))
        ttk.Button(fila, text='Cerrar', command=self.cancelar).pack(side=tk.RIGHT)
        ttk.Button(fila, text='Copiar', command=lambda: self._copiar(texto)).pack(side=tk.RIGHT, padx=(0, 6))
        if ruta_sql and os.path.isfile(ruta_sql):
            ttk.Button(fila, text='Abrir archivo SQL', command=lambda: abrir_en_sistema(ruta_sql)).pack(side=tk.LEFT)

        area = scrolledtext.ScrolledText(self.body, width=90, height=18, wrap=tk.WORD, font=('Consolas', 9))
        area.pack(fill=tk.BOTH, expand=True)
        area.insert('1.0', texto)
        area.config(state=tk.DISABLED)

    def _copiar(self, texto):
        self.clipboard_clear()
        self.clipboard_append(texto)


# ---------------------------------------------------------------------------
# Ventana principal
# ---------------------------------------------------------------------------

class ETLGuiApp:
    """Aplicación principal de la interfaz gráfica del ETL"""

    def __init__(self, root):
        self.root = root
        self.root.title(f"ETL PostgreSQL - LADM-COL v{__version__}")
        escala = root.winfo_fpixels('1i') / 96.0
        self.root.geometry(f"{int(1060 * escala)}x{int(800 * escala)}")
        self.root.minsize(int(880 * escala), int(640 * escala))

        # Cola para comunicar los hilos de trabajo con la GUI (Tk no es seguro entre hilos)
        self.ui_queue = queue.Queue()

        # Estado
        self.etl = None
        self.etl_running = False
        self.flujos = []
        self._nombres_flujo = {}
        self.flujo_actual = None      # flujo de flujos.json o None si las rutas son propias
        self.conexiones = {'origen': {}, 'destino': {}}
        self.estado_conexion = {'origen': None, 'destino': None}
        self.rutas = {}
        self.opciones_extra = {}
        self.esquemas_multi = []
        self.verificacion = None
        self.stats = {}
        self.result_rows = []
        self._filas_por_item = {}
        self._rutas_verificacion = {}
        self.inicio_ejecucion = None

        # Variables de la pantalla
        self.flujo_var = tk.StringVar()
        self.schema_mode = tk.StringVar(value='single')
        self.schema_var = tk.StringVar()
        self.modo_var = tk.StringVar(value='real')
        self.truncate_var = tk.BooleanVar(value=True)
        self.cascade_var = tk.BooleanVar(value=True)
        self.solo_errores = tk.BooleanVar(value=False)

        self.create_menu()
        self.create_main_interface()
        self.cargar_lista_flujos()
        self.load_last_config()
        self.process_ui_queue()

        # Probar las conexiones al iniciar, sin bloquear la ventana
        self.root.after(300, self.test_all_connections)

    # ----------------------------------------------------------------- menú

    def create_menu(self):
        """Crea el menú de la aplicación"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Archivo", menu=file_menu)
        file_menu.add_command(label="Nueva configuración", command=self.new_config)
        file_menu.add_command(label="Abrir configuración...", command=self.load_config, accelerator="Ctrl+O")
        file_menu.add_command(label="Guardar configuración", command=self.save_config, accelerator="Ctrl+S")
        file_menu.add_command(label="Guardar como...", command=self.save_config_as)
        file_menu.add_separator()
        file_menu.add_command(label="Salir", command=self.on_closing, accelerator="Alt+F4")

        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Herramientas", menu=tools_menu)
        tools_menu.add_command(label="Verificar flujo", command=lambda: self.verificar(mostrar=True))
        tools_menu.add_command(label="Probar conexiones", command=self.test_all_connections)
        tools_menu.add_separator()
        tools_menu.add_command(label="Editar lista de flujos (flujos.json)", command=self.editar_flujos)
        tools_menu.add_command(label="Recargar lista de flujos", command=self.recargar_flujos)
        tools_menu.add_separator()
        tools_menu.add_command(label="Abrir carpeta de logs", command=self.open_logs_folder)
        tools_menu.add_command(label="Abrir log de la última ejecución", command=self.open_last_log)

        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Ayuda", menu=help_menu)
        help_menu.add_command(label="Acerca de...", command=self.show_about)

        self.root.bind('<Control-o>', lambda e: self.load_config())
        self.root.bind('<Control-s>', lambda e: self.save_config())

    # ------------------------------------------------------------- pantalla

    def create_main_interface(self):
        """Crea la pantalla principal"""
        main = ttk.Frame(self.root, padding=(14, 10))
        main.pack(fill=tk.BOTH, expand=True)

        # --- Configuración de la corrida
        cfg = ttk.Frame(main)
        cfg.pack(fill=tk.X)
        cfg.columnconfigure(1, weight=1)
        self.cfg_frame = cfg

        ttk.Label(cfg, text='Flujo', style='Field.TLabel').grid(row=0, column=0, sticky='w', padx=(0, 12), pady=4)
        self.flujo_combo = ttk.Combobox(cfg, textvariable=self.flujo_var, state='readonly')
        self.flujo_combo.grid(row=0, column=1, sticky='we', pady=4)
        self.flujo_combo.bind('<<ComboboxSelected>>', self._on_flujo_selected)
        botones = ttk.Frame(cfg)
        botones.grid(row=0, column=2, sticky='e', padx=(8, 0))
        ttk.Button(botones, text='Rutas…', command=self.editar_rutas).pack(side=tk.LEFT)
        ttk.Button(botones, text='Verificar', command=lambda: self.verificar(mostrar=True)).pack(side=tk.LEFT, padx=(4, 0))

        info = ttk.Frame(cfg)
        info.grid(row=1, column=1, columnspan=2, sticky='we', pady=(0, 8))
        self.lbl_flujo_estado = ttk.Label(info, text='')
        self.lbl_flujo_estado.pack(anchor='w')
        self.lbl_flujo_desc = ttk.Label(info, text='', foreground=COLOR['muted'], wraplength=760, justify=tk.LEFT)
        self.lbl_flujo_desc.pack(anchor='w')

        self.lbl_conexion = {}
        self.lbl_estado_conexion = {}
        for fila, tipo in ((2, 'origen'), (3, 'destino')):
            ttk.Label(cfg, text=tipo.capitalize(), style='Field.TLabel').grid(row=fila, column=0, sticky='w', pady=3)
            celda = ttk.Frame(cfg)
            celda.grid(row=fila, column=1, sticky='we')
            self.lbl_conexion[tipo] = ttk.Label(celda, text='')
            self.lbl_conexion[tipo].pack(side=tk.LEFT)
            self.lbl_estado_conexion[tipo] = ttk.Label(celda, text='')
            self.lbl_estado_conexion[tipo].pack(side=tk.LEFT, padx=12)
            botones = ttk.Frame(cfg)
            botones.grid(row=fila, column=2, sticky='e', padx=(8, 0))
            ttk.Button(botones, text='Editar…', command=lambda t=tipo: self.editar_conexion(t)).pack(side=tk.LEFT)
            ttk.Button(botones, text='Probar', command=lambda t=tipo: self.test_connection(t)).pack(side=tk.LEFT, padx=(4, 0))

        ttk.Label(cfg, text='Esquema', style='Field.TLabel').grid(row=4, column=0, sticky='w', pady=(8, 3))
        celda = ttk.Frame(cfg)
        celda.grid(row=4, column=1, columnspan=2, sticky='we', pady=(8, 3))
        ttk.Radiobutton(celda, text='Único:', variable=self.schema_mode, value='single',
                        command=self._on_schema_mode).pack(side=tk.LEFT)
        self.schema_entry = ttk.Entry(celda, textvariable=self.schema_var, width=20)
        self.schema_entry.pack(side=tk.LEFT, padx=(4, 20))
        ttk.Radiobutton(celda, text='Varios:', variable=self.schema_mode, value='multi',
                        command=self._on_schema_mode).pack(side=tk.LEFT)
        self.btn_esquemas = ttk.Button(celda, text='Elegir…', command=self.elegir_esquemas)
        self.btn_esquemas.pack(side=tk.LEFT, padx=(4, 8))
        self.lbl_esquemas = ttk.Label(celda, text='', foreground=COLOR['muted'])
        self.lbl_esquemas.pack(side=tk.LEFT)

        ttk.Label(cfg, text='Modo', style='Field.TLabel').grid(row=5, column=0, sticky='w', pady=3)
        celda = ttk.Frame(cfg)
        celda.grid(row=5, column=1, columnspan=2, sticky='we', pady=3)
        ttk.Radiobutton(celda, text='Real', variable=self.modo_var, value='real',
                        command=self._on_modo).pack(side=tk.LEFT)
        ttk.Radiobutton(celda, text='Simulación (se deshace al final)', variable=self.modo_var, value='simulacion',
                        command=self._on_modo).pack(side=tk.LEFT, padx=(10, 28))
        ttk.Checkbutton(celda, text='Vaciar tablas destino (TRUNCATE)', variable=self.truncate_var,
                        command=self._on_truncate).pack(side=tk.LEFT)
        self.chk_cascade = ttk.Checkbutton(celda, text='CASCADE', variable=self.cascade_var)
        self.chk_cascade.pack(side=tk.LEFT, padx=(8, 0))

        ttk.Separator(main).pack(fill=tk.X, pady=10)

        # --- Acciones y avance
        acciones = ttk.Frame(main)
        acciones.pack(fill=tk.X)
        self.run_button = ttk.Button(acciones, text='▶ Ejecutar', style='Accent.TButton', command=self.run_etl)
        self.run_button.pack(side=tk.LEFT)
        self.stop_button = ttk.Button(acciones, text='■ Detener', command=self.stop_etl, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=6)
        self.lbl_tiempo = ttk.Label(acciones, text='', foreground=COLOR['muted'], width=11, anchor='e')
        self.lbl_tiempo.pack(side=tk.RIGHT)
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(acciones, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)
        self.status_label = ttk.Label(main, text='Listo', foreground=COLOR['muted'])
        self.status_label.pack(anchor='w', pady=(4, 0))

        # --- Resultados / Verificación / Log
        self.notebook = ttk.Notebook(main)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(8, 0))
        self._crear_tab_resultados()
        self._crear_tab_verificacion()
        self._crear_tab_log()

    def _crear_tab_resultados(self):
        frame = ttk.Frame(self.notebook, padding=6)
        self.notebook.add(frame, text='Resultados')
        self.tab_resultados = frame

        pie = ttk.Frame(frame)
        pie.pack(side=tk.BOTTOM, fill=tk.X, pady=(6, 0))
        self.lbl_resumen = ttk.Label(pie, text='Sin ejecuciones en esta sesión.', foreground=COLOR['muted'])
        self.lbl_resumen.pack(side=tk.LEFT)
        ttk.Checkbutton(pie, text='Solo errores', variable=self.solo_errores,
                        command=self._redibujar_resultados).pack(side=tk.RIGHT)

        marco = ttk.Frame(frame)
        marco.pack(fill=tk.BOTH, expand=True)
        columnas = ('esquema', 'fase', 'archivo', 'filas', 'estado', 'tiempo', 'detalle')
        titulos = {'esquema': 'Esquema', 'fase': 'Fase', 'archivo': 'Archivo', 'filas': 'Filas',
                   'estado': 'Estado', 'tiempo': 'Tiempo', 'detalle': 'Detalle (doble clic para ver completo)'}
        anchos = {'esquema': 90, 'fase': 95, 'archivo': 300, 'filas': 80, 'estado': 100, 'tiempo': 70, 'detalle': 320}
        tree = ttk.Treeview(marco, columns=columnas, show='headings', selectmode='browse')
        for columna in columnas:
            tree.heading(columna, text=titulos[columna])
            tree.column(columna, width=anchos[columna], stretch=columna in ('archivo', 'detalle'),
                        anchor='e' if columna in ('filas', 'tiempo') else 'w')
        for tag in ('ok', 'error', 'warning', 'muted'):
            tree.tag_configure(tag, foreground=COLOR[tag])
        barra_v = ttk.Scrollbar(marco, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=barra_v.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        barra_v.pack(side=tk.RIGHT, fill=tk.Y)
        tree.bind('<Double-1>', self._ver_detalle_resultado)
        tree.bind('<Return>', self._ver_detalle_resultado)
        self.results_tree = tree

    def _crear_tab_verificacion(self):
        frame = ttk.Frame(self.notebook, padding=6)
        self.notebook.add(frame, text='Verificación')
        self.tab_verificacion = frame

        self.lbl_verif = ttk.Label(frame, text='Seleccione un flujo para verificarlo.', foreground=COLOR['muted'])
        self.lbl_verif.pack(side=tk.BOTTOM, anchor='w', pady=(6, 0))

        marco = ttk.Frame(frame)
        marco.pack(fill=tk.BOTH, expand=True)
        tree = ttk.Treeview(marco, columns=('detalle',), show='tree headings', selectmode='browse')
        tree.heading('#0', text='Elemento')
        tree.heading('detalle', text='Detalle')
        tree.column('#0', width=380, stretch=False)
        tree.column('detalle', width=460, stretch=True)
        for tag in ('ok', 'error', 'warning', 'muted'):
            tree.tag_configure(tag, foreground=COLOR[tag])
        tree.tag_configure('grupo', font=('Segoe UI', 9, 'bold'))
        barra_v = ttk.Scrollbar(marco, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=barra_v.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        barra_v.pack(side=tk.RIGHT, fill=tk.Y)
        tree.bind('<Double-1>', self._abrir_archivo_verificacion)
        self.verif_tree = tree

    def _crear_tab_log(self):
        frame = ttk.Frame(self.notebook, padding=6)
        self.notebook.add(frame, text='Log')
        self.tab_log = frame

        barra = ttk.Frame(frame)
        barra.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(barra, text='Limpiar', command=self.clear_logs).pack(side=tk.LEFT)
        ttk.Button(barra, text='Abrir carpeta de logs', command=self.open_logs_folder).pack(side=tk.LEFT, padx=4)
        ttk.Button(barra, text='Abrir log de la última ejecución', command=self.open_last_log).pack(side=tk.LEFT)

        self.log_text = scrolledtext.ScrolledText(frame, height=12, wrap=tk.WORD, font=('Consolas', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED)
        self.log_text.tag_config('error', foreground=COLOR['error'])
        self.log_text.tag_config('warning', foreground=COLOR['warning'])
        self.log_text.tag_config('ok', foreground=COLOR['ok'])
        self.log_text.tag_config('info', foreground='')

    # ------------------------------------------------------------ hilos

    def run_in_background(self, funcion, al_terminar):
        """Ejecuta funcion en un hilo y llama al_terminar(resultado, error) en el hilo de la GUI"""
        def trabajo():
            try:
                resultado, error = funcion(), None
            except Exception as e:
                resultado, error = None, e
            self.ui_queue.put(('call', lambda: al_terminar(resultado, error)))
        threading.Thread(target=trabajo, daemon=True).start()

    def process_ui_queue(self):
        """Procesa los mensajes de los hilos de trabajo (se ejecuta cada 100 ms)"""
        try:
            for _ in range(500):
                tipo, datos = self.ui_queue.get_nowait()
                try:
                    if tipo == 'log':
                        self._append_log(*datos)
                    elif tipo == 'event':
                        self._on_etl_event(*datos)
                    elif tipo == 'call':
                        datos()
                    elif tipo == 'finished':
                        self._on_etl_finished(*datos)
                except Exception as e:
                    self._append_log(f"[GUI] Error procesando {tipo}: {e}\n", 'error')
        except queue.Empty:
            pass
        finally:
            if self.etl_running and self.inicio_ejecucion:
                self.lbl_tiempo.config(text='⏱ ' + formato_duracion(time.time() - self.inicio_ejecucion))
            self.root.after(100, self.process_ui_queue)

    # ------------------------------------------------------------ flujos

    def cargar_lista_flujos(self):
        """Lee config/flujos.json y llena el selector"""
        self.flujos = []
        if FLUJOS_FILE.exists():
            try:
                self.flujos = cargar_flujos(str(FLUJOS_FILE), str(BASE_DIR))
            except Exception as e:
                self.log_message(f"No se pudo leer {FLUJOS_FILE}: {e}", 'error')
        else:
            self.log_message(f"No existe {FLUJOS_FILE}; solo hay rutas manuales", 'warning')

        self._nombres_flujo = {}
        for flujo in sorted(self.flujos, key=lambda f: not f['vigente']):
            nombre = flujo['nombre'] if flujo['vigente'] else f"{flujo['nombre']} (histórico)"
            self._nombres_flujo[nombre] = flujo
        self.flujo_combo['values'] = list(self._nombres_flujo) + [PERSONALIZADO]

    def recargar_flujos(self):
        self.cargar_lista_flujos()
        self._sincronizar_flujo()
        self.verificar()
        self.log_message(f"Lista de flujos recargada ({len(self.flujos)} flujos)", 'info')

    def editar_flujos(self):
        if FLUJOS_FILE.exists():
            abrir_en_sistema(str(FLUJOS_FILE))
            messagebox.showinfo('Lista de flujos',
                                'Después de guardar los cambios use Herramientas > Recargar lista de flujos.')
        else:
            messagebox.showwarning('Archivo no encontrado', f'No existe {FLUJOS_FILE}')

    def _sincronizar_flujo(self):
        """Selecciona en el combo el flujo que corresponde a las rutas actuales"""
        self.flujo_actual = identificar_flujo(self.flujos, self.rutas.get('queries', ''),
                                              self.rutas.get('inserts', ''), self.rutas.get('order_file', ''))
        nombre = next((n for n, f in self._nombres_flujo.items() if f is self.flujo_actual), PERSONALIZADO)
        self.flujo_var.set(nombre)

    def _on_flujo_selected(self, event=None):
        flujo = self._nombres_flujo.get(self.flujo_var.get())
        if flujo is None:
            self.editar_rutas()
            return
        self.flujo_actual = flujo
        self.rutas.update(queries=flujo['queries'], inserts=flujo['inserts'], order_file=flujo['orden'])
        self.verificar()

    def editar_rutas(self):
        resultado = RutasDialogo(self, self.rutas).mostrar()
        if resultado:
            self.rutas.update(resultado)
        self._sincronizar_flujo()
        self.verificar()

    def _nombre_flujo(self):
        return self.flujo_actual['nombre'] if self.flujo_actual else PERSONALIZADO

    def _esquema_referencia(self):
        """Esquema usado para listar las tablas destino en la verificación"""
        if self.schema_mode.get() == 'multi':
            return self.esquemas_multi[0] if self.esquemas_multi else 'esquema'
        return self.schema_var.get().strip() or 'esquema'

    def verificar(self, mostrar=False):
        """Revisa el flujo (archivos, orden, dependencias, tablas a vaciar) sin conectarse"""
        try:
            resultado = verificar_flujo(self.rutas.get('queries', ''), self.rutas.get('inserts', ''),
                                        self.rutas.get('order_file', ''), self._esquema_referencia())
        except Exception as e:
            resultado = {'queries': [], 'inserts': [], 'desactivados': [], 'sin_orden': [],
                         'tablas_destino': [], 'orden_alfabetico': False,
                         'errores': [f'No se pudo verificar: {e}'], 'advertencias': []}
        self.verificacion = resultado
        self._mostrar_verificacion(resultado)
        self._actualizar_info_flujo()
        if mostrar:
            self.notebook.select(self.tab_verificacion)
        return resultado

    def _actualizar_info_flujo(self):
        v = self.verificacion or {}
        desc = self.flujo_actual['descripcion'] if self.flujo_actual else 'Rutas definidas manualmente (botón Rutas…).'
        self.lbl_flujo_desc.config(text=desc)
        errores, advertencias = v.get('errores', []), v.get('advertencias', [])
        if errores:
            texto, color = f"✖ {errores[0]}", COLOR['error']
        else:
            partes = [f"{len(v.get('queries', []))} queries", f"{len(v.get('inserts', []))} inserts"]
            if v.get('desactivados'):
                partes.append(f"{len(v['desactivados'])} desactivados")
            orden = os.path.basename(self.rutas.get('order_file', '')) or 'alfabético'
            partes.append(f"orden: {orden}")
            if advertencias:
                texto = f"⚠ {' · '.join(partes)} · {len(advertencias)} advertencia(s), ver Verificación"
                color = COLOR['warning']
            else:
                texto, color = f"✔ {' · '.join(partes)}", COLOR['ok']
        self.lbl_flujo_estado.config(text=texto, foreground=color)

    def _mostrar_verificacion(self, v):
        tree = self.verif_tree
        tree.delete(*tree.get_children())
        self._rutas_verificacion = {}

        def grupo(titulo, cantidad, abierto=True, tag='grupo'):
            return tree.insert('', tk.END, text=f'{titulo} ({cantidad})', open=abierto, tags=(tag,))

        if v['errores']:
            nodo = grupo('Errores', len(v['errores']))
            for error in v['errores']:
                tree.insert(nodo, tk.END, text=primera_linea(error, 90), values=(error,), tags=('error',))
        if v['advertencias']:
            nodo = grupo('Advertencias', len(v['advertencias']))
            for aviso in v['advertencias']:
                archivo, _, detalle = aviso.partition(': ')
                tree.insert(nodo, tk.END, text=archivo if detalle else primera_linea(aviso, 90),
                            values=(detalle or aviso,), tags=('warning',))

        if v['inserts']:
            titulo = 'Inserts en orden alfabético' if v['orden_alfabetico'] else 'Inserts en orden de ejecución'
            nodo = grupo(titulo, len(v['inserts']))
            for numero, item in enumerate(v['inserts'], start=1):
                texto_estado, tag = ESTADOS.get(item['estado'], (item['estado'], 'muted'))
                iid = tree.insert(nodo, tk.END, text=f"{numero:>3}. {item['archivo']}",
                                  values=(item['detalle'] or texto_estado,), tags=(tag,))
                self._rutas_verificacion[iid] = os.path.join(self.rutas.get('inserts', ''), item['archivo'])
        if v['desactivados']:
            nodo = grupo("Desactivados en el orden (prefijo '-')", len(v['desactivados']), abierto=False)
            for archivo in v['desactivados']:
                tree.insert(nodo, tk.END, text=archivo, values=('No se ejecuta',), tags=('muted',))
        if v['sin_orden']:
            nodo = grupo('En la carpeta pero no en el orden', len(v['sin_orden']), abierto=False)
            for archivo in v['sin_orden']:
                iid = tree.insert(nodo, tk.END, text=archivo, values=('No se ejecuta',), tags=('muted',))
                self._rutas_verificacion[iid] = os.path.join(self.rutas.get('inserts', ''), archivo)
        if v['queries']:
            nodo = grupo('Queries (origen → tabla temporal)', len(v['queries']), abierto=False)
            for archivo in v['queries']:
                iid = tree.insert(nodo, tk.END, text=archivo, values=(f"tmp_{os.path.splitext(archivo)[0]}",))
                self._rutas_verificacion[iid] = os.path.join(self.rutas.get('queries', ''), archivo)
        if v['tablas_destino']:
            nodo = grupo(f"Tablas que se vaciarán con TRUNCATE (esquema {self._esquema_referencia()})",
                         len(v['tablas_destino']), abierto=False)
            for tabla in v['tablas_destino']:
                tree.insert(nodo, tk.END, text=tabla, values=('',))

        if v['errores']:
            self.lbl_verif.config(text=f"✖ El flujo tiene {len(v['errores'])} error(es); no se puede ejecutar.",
                                  foreground=COLOR['error'])
        elif v['advertencias']:
            self.lbl_verif.config(text=f"⚠ {len(v['advertencias'])} advertencia(s). Doble clic en un archivo para abrirlo.",
                                  foreground=COLOR['warning'])
        else:
            self.lbl_verif.config(text='✔ Sin problemas. Doble clic en un archivo para abrirlo.', foreground=COLOR['ok'])

    def _abrir_archivo_verificacion(self, event):
        ruta = self._rutas_verificacion.get(self.verif_tree.identify_row(event.y))
        if ruta and os.path.isfile(ruta):
            abrir_en_sistema(ruta)

    # ------------------------------------------------------- conexiones

    def editar_conexion(self, tipo):
        resultado = ConexionDialogo(self, tipo, self.conexiones[tipo]).mostrar()
        if resultado is None:
            return
        valores, probado = resultado
        if valores != self.conexiones[tipo]:
            self.estado_conexion[tipo] = None
        self.conexiones[tipo] = valores
        if probado is not None:
            self.estado_conexion[tipo] = probado
        self._actualizar_conexiones()

    def _actualizar_conexiones(self):
        for tipo in ('origen', 'destino'):
            self.lbl_conexion[tipo].config(text=resumen_conexion(self.conexiones[tipo]))
            estado = self.estado_conexion[tipo]
            if estado == 'probando':
                texto, color = '● probando…', COLOR['muted']
            elif estado is True:
                texto, color = '● conectado', COLOR['ok']
            elif estado is False:
                texto, color = '● sin conexión (ver Log)', COLOR['error']
            else:
                texto, color = '● sin probar', COLOR['muted']
            self.lbl_estado_conexion[tipo].config(text=texto, foreground=color)

    def test_connection(self, tipo):
        """Prueba una conexión en segundo plano y muestra el resultado junto a ella"""
        valores = dict(self.conexiones[tipo])
        if not valores.get('database'):
            self.estado_conexion[tipo] = None
            self._actualizar_conexiones()
            return
        try:
            config_conexion(valores)
        except ValueError as e:
            self.estado_conexion[tipo] = False
            self.log_message(f"Conexión {tipo}: {e}", 'error')
            self._actualizar_conexiones()
            return

        self.estado_conexion[tipo] = 'probando'
        self._actualizar_conexiones()

        def terminado(version, error):
            if valores != self.conexiones[tipo]:
                return  # Los datos cambiaron mientras se probaba
            if error:
                self.estado_conexion[tipo] = False
                self.log_message(f"❌ Conexión {tipo} ({resumen_conexion(valores)}): {str(error).strip()}", 'error')
            else:
                self.estado_conexion[tipo] = True
                self.log_message(f"✅ Conexión {tipo} ({resumen_conexion(valores)}): {version}", 'ok')
            self._actualizar_conexiones()

        self.run_in_background(lambda: probar_conexion(valores), terminado)

    def test_all_connections(self):
        self.test_connection('origen')
        self.test_connection('destino')

    # --------------------------------------------------- esquemas y modo

    def _on_schema_mode(self):
        multi = self.schema_mode.get() == 'multi'
        self.schema_entry.state(['disabled'] if multi else ['!disabled'])
        self.btn_esquemas.state(['!disabled'] if multi else ['disabled'])
        self._actualizar_lbl_esquemas()

    def _actualizar_lbl_esquemas(self):
        esquemas = self.esquemas_multi
        if not esquemas:
            texto = 'ninguno seleccionado'
        else:
            texto = f"{len(esquemas)} seleccionado(s): {', '.join(esquemas[:4])}{'…' if len(esquemas) > 4 else ''}"
        self.lbl_esquemas.config(text=texto)

    def elegir_esquemas(self):
        resultado = EsquemasDialogo(self, self.esquemas_multi).mostrar()
        if resultado is not None:
            self.esquemas_multi = resultado
            self._actualizar_lbl_esquemas()
            self.verificar()

    def _on_modo(self):
        simular = self.modo_var.get() == 'simulacion'
        self.run_button.config(text='▶ Simular' if simular else '▶ Ejecutar')

    def _on_truncate(self):
        self.chk_cascade.state(['!disabled'] if self.truncate_var.get() else ['disabled'])

    def _esquemas_a_procesar(self):
        if self.schema_mode.get() == 'multi':
            return list(self.esquemas_multi)
        return [self.schema_var.get().strip()]

    def _set_config_enabled(self, habilitado):
        """Bloquea la configuración mientras corre el ETL"""
        def recorrer(widget):
            for hijo in widget.winfo_children():
                if isinstance(hijo, (ttk.Button, ttk.Entry, ttk.Radiobutton, ttk.Checkbutton)):
                    hijo.state(['!disabled'] if habilitado else ['disabled'])
                recorrer(hijo)
        recorrer(self.cfg_frame)
        if habilitado:
            self._on_schema_mode()
            self._on_truncate()

    # ------------------------------------------------------ configuración

    def get_config_dict(self):
        """Obtiene la configuración actual como diccionario (formato de etl_config.json)"""
        return {
            'flujo': self.flujo_actual['id'] if self.flujo_actual else '',
            'origen': dict(self.conexiones['origen']),
            'destino': dict(self.conexiones['destino']),
            'schema': self.schema_var.get().strip(),
            'paths': {
                'queries': self.rutas.get('queries', ''),
                'inserts': self.rutas.get('inserts', ''),
                'order_file': self.rutas.get('order_file', ''),
                'logs': self.rutas.get('logs', ''),
            },
            'options': {
                'batch_size': self.opciones_extra.get('batch_size', '1000'),
                'pool_size': self.opciones_extra.get('pool_size', '5'),
                'truncate_enabled': self.truncate_var.get(),
                'truncate_cascade': self.cascade_var.get(),
                'dry_run': self.modo_var.get() == 'simulacion',
                'schema_mode': self.schema_mode.get(),
            },
            'schemas': list(self.esquemas_multi),
        }

    def set_config_from_dict(self, config):
        """Aplica una configuración (acepta también el formato de versiones anteriores)"""
        base = copy.deepcopy(DEFAULT_CONFIG)

        for tipo in ('origen', 'destino'):
            valores = base[tipo]
            for clave, valor in (config.get(tipo) or {}).items():
                if clave in valores:
                    valores[clave] = '' if valor is None else str(valor)
            if valores != self.conexiones.get(tipo):
                self.estado_conexion[tipo] = None
            self.conexiones[tipo] = valores

        # Retrocompatibilidad: el esquema estuvo dentro de 'destino'
        self.schema_var.set(config.get('schema') or (config.get('destino') or {}).get('schema') or '')

        rutas = base['paths']
        rutas.update({k: v for k, v in (config.get('paths') or {}).items() if k in rutas and v is not None})
        self.rutas = rutas

        opciones = base['options']
        opciones.update(config.get('options') or {})
        self.opciones_extra = {'batch_size': str(opciones['batch_size']), 'pool_size': str(opciones['pool_size'])}
        self.truncate_var.set(bool(opciones['truncate_enabled']))
        self.cascade_var.set(bool(opciones['truncate_cascade']))
        self.modo_var.set('simulacion' if opciones.get('dry_run') else 'real')
        self.schema_mode.set('multi' if opciones.get('schema_mode') == 'multi' else 'single')
        self.esquemas_multi = list(config.get('schemas') or [])

        # Si el flujo guardado existe se toman sus rutas actuales de flujos.json
        flujo = next((f for f in self.flujos if f['id'] == config.get('flujo')), None)
        if flujo:
            self.rutas.update(queries=flujo['queries'], inserts=flujo['inserts'], order_file=flujo['orden'])

        self._sincronizar_flujo()
        self._on_schema_mode()
        self._on_truncate()
        self._on_modo()
        self._actualizar_conexiones()
        self.verificar()

    def new_config(self):
        """Vuelve a los valores por defecto"""
        if messagebox.askyesno("Nueva configuración",
                               "¿Desea volver a la configuración por defecto? Se perderán los valores actuales."):
            self.set_config_from_dict({})
            self.log_message("Nueva configuración creada", 'info')

    def save_config(self, silencioso=False):
        """Guarda la configuración actual en etl_config.json"""
        self._save_config_to_file(str(CONFIG_FILE), silencioso)

    def save_config_as(self):
        file = filedialog.asksaveasfilename(
            initialdir=str(BASE_DIR), title="Guardar configuración como", defaultextension=".json",
            filetypes=[("Archivos JSON", "*.json"), ("Todos los archivos", "*.*")])
        if file:
            self._save_config_to_file(file)

    def _save_config_to_file(self, filepath, silencioso=False):
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.get_config_dict(), f, indent=4, ensure_ascii=False)
            if not silencioso:
                self.log_message(f"Configuración guardada en: {filepath}", 'ok')
        except Exception as e:
            self.log_message(f"Error al guardar configuración: {e}", 'error')
            if not silencioso:
                messagebox.showerror("Error", f"No se pudo guardar la configuración:\n{e}")

    def load_config(self):
        file = filedialog.askopenfilename(
            initialdir=str(BASE_DIR), title="Abrir configuración",
            filetypes=[("Archivos JSON", "*.json"), ("Todos los archivos", "*.*")])
        if file:
            self._load_config_from_file(file)
            self.test_all_connections()

    def _load_config_from_file(self, filepath):
        try:
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                config = json.load(f)
            self.set_config_from_dict(config)
            self.log_message(f"Configuración cargada desde: {filepath}", 'ok')
        except Exception as e:
            self.log_message(f"Error al cargar configuración: {e}", 'error')
            messagebox.showerror("Error", f"No se pudo cargar la configuración:\n{e}")

    def load_last_config(self):
        """Carga etl_config.json si existe; si no, los valores por defecto"""
        if CONFIG_FILE.exists():
            self._load_config_from_file(str(CONFIG_FILE))
        else:
            self.set_config_from_dict({})

    # ---------------------------------------------------------------- log

    def log_message(self, message, level='info'):
        """Agrega un mensaje de la GUI al log"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.ui_queue.put(('log', (f"{timestamp}  {message}\n", level)))

    def _append_log(self, mensaje, nivel):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, mensaje, nivel)
        # Limitar el tamaño del visor (el archivo de log conserva todo)
        if int(self.log_text.index('end-1c').split('.')[0]) > 20000:
            self.log_text.delete('1.0', '5000.0')
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)

    def clear_logs(self):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)

    def open_logs_folder(self):
        logs_path = self.rutas.get('logs', '')
        if os.path.isdir(logs_path):
            abrir_en_sistema(logs_path)
        else:
            messagebox.showwarning("Carpeta no encontrada", f"La carpeta de logs no existe:\n{logs_path}")

    def open_last_log(self):
        archivos = glob.glob(os.path.join(self.rutas.get('logs', ''), 'etl_execution_*.txt'))
        if archivos:
            abrir_en_sistema(max(archivos, key=os.path.getmtime))
        else:
            messagebox.showinfo("Sin logs", "Todavía no hay logs de ejecución en la carpeta de logs.")

    # ------------------------------------------------------------ ejecutar

    def validate_config(self):
        """Valida los datos mínimos antes de ejecutar"""
        errores = []
        for tipo in ('origen', 'destino'):
            c = self.conexiones[tipo]
            if not c.get('host') or not c.get('database'):
                errores.append(f"Falta el host o la base de datos {tipo} (botón Editar…)")
            else:
                try:
                    config_conexion(c)
                except ValueError as e:
                    errores.append(f"Conexión {tipo}: {e}")
        if self.schema_mode.get() == 'single':
            if not self.schema_var.get().strip():
                errores.append("Escriba el esquema a migrar")
        elif not self.esquemas_multi:
            errores.append("Elija al menos un esquema (botón Elegir…)")
        if not self.rutas.get('logs'):
            errores.append("Falta la carpeta de logs (botón Rutas…)")
        return errores

    def run_etl(self):
        """Valida, pide confirmación y ejecuta el ETL en un hilo"""
        if self.etl_running:
            return
        errores = self.validate_config()
        if errores:
            messagebox.showerror("Faltan datos", "Corrija lo siguiente:\n\n• " + "\n• ".join(errores))
            return

        verificacion = self.verificar()
        if verificacion['errores']:
            self.notebook.select(self.tab_verificacion)
            messagebox.showerror("El flujo tiene errores",
                                 "Corrija lo siguiente antes de ejecutar:\n\n• " + "\n• ".join(verificacion['errores']))
            return

        simular = self.modo_var.get() == 'simulacion'
        schemas = self._esquemas_a_procesar()
        datos = {
            'simular': simular,
            'flujo': self._nombre_flujo(),
            'origen': resumen_conexion(self.conexiones['origen']),
            'destino': resumen_conexion(self.conexiones['destino']),
            'esquemas': schemas,
            'truncate': self.truncate_var.get(),
            'cascade': self.cascade_var.get(),
            'tablas': verificacion['tablas_destino'],
            'advertencias': len(verificacion['advertencias']),
        }
        if not ConfirmarDialogo(self, datos).mostrar():
            return

        self.save_config(silencioso=True)

        # Todo lo que usa el hilo se lee aquí (Tk no es seguro entre hilos)
        params = {
            'origen': config_conexion(self.conexiones['origen']),
            'destino': config_conexion(self.conexiones['destino']),
            'paths': {
                'queries': self.rutas['queries'],
                'inserts': self.rutas['inserts'],
                'logs': self.rutas['logs'],
                'order_file': self.rutas.get('order_file', ''),
            },
            'performance': {
                'batch_size': entero(self.opciones_extra.get('batch_size'), 1000),
                'connection_pool_size': entero(self.opciones_extra.get('pool_size'), 5),
                'max_retries': 3,
                'timeout': 300,
            },
            'truncate': {
                'enabled': self.truncate_var.get(),
                'require_confirmation': False,  # Ya se confirmó en la GUI
                'use_cascade': self.cascade_var.get(),
                'skip_if_not_exists': True,
            },
            'schemas': schemas,
            'simular': simular,
        }

        self._preparar_ejecucion(params)
        threading.Thread(target=self._run_etl_thread, args=(params,), daemon=True).start()

    def _preparar_ejecucion(self, params):
        self.stats = {
            'simular': params['simular'], 'schema_idx': 0, 'schema_total': len(params['schemas']),
            'queries_ok': 0, 'queries_error': 0, 'inserts_ok': 0, 'inserts_error': 0, 'inserts_omitidos': 0,
            'filas': 0, 'stop_requested': False, 'paths': params['paths'],
        }
        self.result_rows = []
        self._redibujar_resultados()
        self.etl_running = True
        self.inicio_ejecucion = time.time()
        self.run_button.state(['disabled'])
        self.stop_button.state(['!disabled'])
        self._set_config_enabled(False)
        self.progress_var.set(0)
        self._set_status('Conectando a las bases de datos…')
        self._actualizar_resumen()
        self.notebook.select(self.tab_resultados)

        modo = 'SIMULACIÓN' if params['simular'] else 'EJECUCIÓN'
        self.log_message('=' * 70)
        self.log_message(f"{modo}: {self._nombre_flujo()} · esquemas: {', '.join(params['schemas'])}", 'info')
        self.log_message('=' * 70)

    def _run_etl_thread(self, params):
        """Ejecuta el ETL en un hilo separado"""
        handler = GuiLogHandler(self.ui_queue)
        etl_logger = logging.getLogger('ETL')
        etl_logger.addHandler(handler)
        try:
            etl = ETLProcessor(
                origen_config=params['origen'],
                destino_config=params['destino'],
                paths=params['paths'],
                performance_config=params['performance'],
                truncate_config=params['truncate'],
                simulate=params['simular'],
                on_event=lambda evento, datos: self.ui_queue.put(('event', (evento, datos))),
            )
            self.etl = etl
            if self.stats.get('stop_requested'):
                etl.request_stop()
            ok = etl.run_etl(params['schemas'], on_schema_error=self._preguntar_continuar)
            self.ui_queue.put(('finished', (ok, etl.last_summary, None)))
        except Exception as e:
            self.ui_queue.put(('finished', (False, None, str(e))))
        finally:
            etl_logger.removeHandler(handler)

    def _preguntar_continuar(self, schema, error_msg):
        """Se llama desde el hilo del ETL cuando falla un esquema (modo varios esquemas)"""
        respuesta = [False]
        listo = threading.Event()

        def preguntar():
            respuesta[0] = messagebox.askyesno(
                "Error en esquema",
                f"El esquema '{schema}' falló:\n{error_msg}\n\n¿Desea continuar con los esquemas restantes?")
            listo.set()

        self.ui_queue.put(('call', preguntar))
        listo.wait()
        return respuesta[0]

    def _set_status(self, texto):
        self.status_label.config(text=texto)

    def _set_progreso(self, avance_esquema):
        total = max(self.stats.get('schema_total', 1), 1)
        self.progress_var.set((self.stats.get('schema_idx', 0) + avance_esquema) / total * 100)

    def _on_etl_event(self, evento, d):
        s = self.stats
        if evento == 'schema_start':
            s['schema_idx'] = d['indice']
            self._set_progreso(0.0)
            self._set_status(f"Esquema {d['indice'] + 1}/{d['total']}: {d['schema']}")
        elif evento == 'phase':
            self._set_progreso(AVANCE_FASE.get(d['fase'], 0.0))
            self._set_status(f"{d['schema']} · {FASES.get(d['fase'], d['fase'])}…")
        elif evento == 'query_start':
            self._set_progreso(0.05 + 0.30 * d['indice'] / max(d['total'], 1))
            self._set_status(f"{d['schema']} · Query {d['indice'] + 1}/{d['total']}: {d['archivo']}")
        elif evento == 'query_done':
            if d['estado'] == 'ok':
                s['queries_ok'] += 1
            elif d['estado'] == 'error':
                s['queries_error'] += 1
            self._agregar_resultado(d, 'Query', self.stats['paths']['queries'])
        elif evento == 'insert_start':
            self._set_progreso(0.40 + 0.59 * d['indice'] / max(d['total'], 1))
            self._set_status(f"{d['schema']} · Insert {d['indice'] + 1}/{d['total']}: {d['archivo']}")
        elif evento == 'insert_done':
            if d['estado'] == 'ok':
                s['inserts_ok'] += 1
                s['filas'] += max(d['filas'] or 0, 0)
            elif d['estado'] == 'error':
                s['inserts_error'] += 1
            elif d['estado'] == 'omitido':
                s['inserts_omitidos'] += 1
            self._agregar_resultado(d, 'Insert', self.stats['paths']['inserts'])
        elif evento == 'truncate_done':
            detalle = f"Vaciadas: {d['truncadas']} · omitidas (no existen): {d['omitidas']} · con error: {d['fallidas']}"
            if d['tablas_fallidas']:
                detalle += '\nTablas con error: ' + ', '.join(d['tablas_fallidas'])
            fila = {'schema': d['schema'], 'archivo': f"{d['truncadas'] + d['omitidas'] + d['fallidas']} tablas",
                    'filas': None, 'estado': 'error' if d['fallidas'] else 'ok', 'segundos': None, 'error': detalle}
            self._agregar_resultado(fila, 'TRUNCATE', None)
        self._actualizar_resumen()

    def _agregar_resultado(self, d, fase, carpeta):
        fila = {
            'schema': d['schema'], 'fase': fase, 'archivo': d['archivo'], 'filas': d.get('filas'),
            'estado': d['estado'], 'segundos': d.get('segundos'), 'error': d.get('error'),
            'ruta': os.path.join(carpeta, d['archivo']) if carpeta else None,
        }
        self.result_rows.append(fila)
        if self._fila_visible(fila):
            item = self._insertar_fila(fila)
            self.results_tree.see(item)

    def _fila_visible(self, fila):
        return not self.solo_errores.get() or fila['estado'] in ('error', 'cancelado')

    def _insertar_fila(self, fila):
        texto_estado, tag = ESTADOS.get(fila['estado'], (fila['estado'], 'muted'))
        item = self.results_tree.insert('', tk.END, tags=(tag,), values=(
            fila['schema'], fila['fase'], fila['archivo'], formato_filas(fila['filas']), texto_estado,
            formato_tiempo(fila['segundos']), primera_linea(fila['error'])))
        self._filas_por_item[item] = fila
        return item

    def _redibujar_resultados(self):
        self.results_tree.delete(*self.results_tree.get_children())
        self._filas_por_item = {}
        for fila in self.result_rows:
            if self._fila_visible(fila):
                self._insertar_fila(fila)

    def _actualizar_resumen(self):
        s = self.stats
        if not s:
            return
        partes = [f"Queries: {s['queries_ok']} ✔" + (f" / {s['queries_error']} ✖" if s['queries_error'] else ''),
                  f"Inserts: {s['inserts_ok']} ✔ / {s['inserts_error']} ✖"
                  + (f" / {s['inserts_omitidos']} omitidos" if s['inserts_omitidos'] else ''),
                  f"{formato_miles(s['filas'])} filas cargadas"]
        texto = ' · '.join(partes)
        if s['simular']:
            texto += ' · SIMULACIÓN (no se guarda)'
        color = COLOR['error'] if s['inserts_error'] or s['queries_error'] else COLOR['muted']
        self.lbl_resumen.config(text=texto, foreground=color)

    def _ver_detalle_resultado(self, event):
        if event.type == tk.EventType.KeyPress:
            seleccion = (self.results_tree.selection() or [None])[0]
        else:
            seleccion = self.results_tree.identify_row(event.y)
        fila = self._filas_por_item.get(seleccion)
        if not fila:
            return
        texto_estado = ESTADOS.get(fila['estado'], (fila['estado'], ''))[0]
        texto = (f"Esquema: {fila['schema']}\nFase: {fila['fase']}\nArchivo: {fila['archivo']}\n"
                 f"Estado: {texto_estado}\nFilas: {formato_filas(fila['filas']) or '-'}\n"
                 f"Tiempo: {formato_tiempo(fila['segundos']) or '-'}\n\n{fila['error'] or 'Sin errores.'}")
        DetalleDialogo(self, fila['archivo'], texto, fila['ruta']).mostrar()

    def _on_etl_finished(self, ok, summary, error_critico):
        s = self.stats
        self.etl_running = False
        self.etl = None
        self.run_button.state(['!disabled'])
        self.stop_button.state(['disabled'])
        self._set_config_enabled(True)
        duracion = formato_duracion(time.time() - self.inicio_ejecucion) if self.inicio_ejecucion else ''
        self.lbl_tiempo.config(text='⏱ ' + duracion if duracion else '')
        simular = s.get('simular')
        nota = "\n\nNo se guardó ningún cambio en la base destino." if simular else ''
        errores = s.get('inserts_error', 0) + s.get('queries_error', 0)
        titulo = 'Simulación' if simular else 'ETL'
        o = 'a' if simular else 'o'  # concordancia: "Simulación completada" / "ETL completado"

        if error_critico:
            self._set_status(f"Error crítico: {error_critico}")
            self.notebook.select(self.tab_log)
            messagebox.showerror("Error", f"Error durante la ejecución:\n{error_critico}")
        elif summary is None:
            self._set_status("No se pudo iniciar: revise el Log")
            self.notebook.select(self.tab_log)
            messagebox.showerror(f"{titulo} no iniciad{o}",
                                 "El proceso no pudo iniciar (conexión o carpetas). Revise la pestaña Log.")
        elif summary.get('stopped_by_user'):
            self._set_status(f"Detenido por el usuario · {duracion}")
            messagebox.showinfo(f"{titulo} detenid{o}",
                                "El proceso se detuvo." + (nota if simular else
                                "\n\nLos archivos que terminaron antes de detener quedaron guardados."))
        elif errores or not ok:
            self._set_status(f"Terminó con errores · {duracion}")
            self.notebook.select(self.tab_resultados)
            detalle = f"{errores} archivo(s) con error" if errores else "errores en el procesamiento"
            messagebox.showwarning(f"{titulo} con errores",
                                   f"El proceso terminó con {detalle}.\n\nEn la pestaña Resultados marque "
                                   f"'Solo errores' y haga doble clic en una fila para ver el error completo.{nota}")
        else:
            self.progress_var.set(100)
            self._set_status(f"{'Simulación completada' if simular else 'Completado'} · {duracion}")
            messagebox.showinfo(f"{titulo} completad{o}",
                                f"Esquemas procesados: {summary['success_count']}\n"
                                f"Inserts: {s['inserts_ok']} · Filas cargadas: {formato_miles(s['filas'])}{nota}")

    def stop_etl(self):
        """Detiene el proceso: cancela la sentencia en curso y no inicia archivos nuevos"""
        if not self.etl_running:
            return
        if self.stats.get('simular'):
            aviso = "Se cancela la sentencia en curso y se deshace todo."
        else:
            aviso = ("Se cancela la sentencia en curso. Los archivos que ya terminaron "
                     "quedan guardados en la base destino.")
        if not messagebox.askyesno("Detener", f"¿Desea detener el proceso?\n\n{aviso}"):
            return
        self.stats['stop_requested'] = True
        self.stop_button.state(['disabled'])
        self._set_status("Deteniendo… (esperando a que la base cancele la sentencia en curso)")
        self.log_message("⏹️ Detención solicitada por el usuario", 'warning')
        etl = self.etl
        if etl:
            threading.Thread(target=etl.request_stop, daemon=True).start()

    # -------------------------------------------------------------- otros

    def show_about(self):
        messagebox.showinfo("Acerca de", f"""ETL PostgreSQL - LADM-COL
Versión {__version__}

Migración de datos catastrales entre modelos LADM-COL.

• Flujos predefinidos en config/flujos.json
• Verificación del flujo antes de ejecutar
• Simulación con ROLLBACK (no guarda cambios)
• Resultados por archivo y log en tiempo real
• Varios esquemas en una sola corrida""")

    def on_closing(self):
        """Maneja el cierre de la aplicación"""
        if self.etl_running:
            if not messagebox.askyesno("Confirmar cierre",
                                       "Hay un proceso ETL en ejecución. Si cierra, se cancela la sentencia "
                                       "en curso. ¿Desea cerrar de todos modos?"):
                return
            if self.etl:
                self.etl.request_stop()

        self.save_config(silencioso=True)
        self.root.destroy()


def main():
    """Función principal"""
    if sys.platform == 'win32':
        # Texto nítido en pantallas con escalado
        try:
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            pass

    root = tk.Tk()
    escala = root.winfo_fpixels('1i') / 96.0

    style = ttk.Style()
    style.theme_use('vista' if 'vista' in style.theme_names() else 'clam')
    style.configure('Accent.TButton', font=('Segoe UI', 10, 'bold'))
    style.configure('Title.TLabel', font=('Segoe UI', 11, 'bold'))
    style.configure('Field.TLabel', font=('Segoe UI', 9, 'bold'))
    style.configure('Treeview', rowheight=int(22 * escala))

    app = ETLGuiApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
