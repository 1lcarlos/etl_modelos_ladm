# gui.py
"""
Interfaz Gráfica para ETL PostgreSQL - LADM-COL
Permite configurar y ejecutar el proceso ETL de forma visual

Autor: ETL Team
Versión: 1.0.0
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import json
import os
import sys
import threading
import queue
from pathlib import Path
from datetime import datetime

# Configurar codificación UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Directorio base del proyecto
BASE_DIR = Path(__file__).resolve().parent

# Imports locales
sys.path.insert(0, str(BASE_DIR))
from src.etl_processor import ETLProcessor
from src.database_manager import DatabaseManager


class LogHandler:
    """Manejador de logs para capturar salida y mostrar en GUI"""

    def __init__(self, text_widget, log_queue):
        self.text_widget = text_widget
        self.log_queue = log_queue

    def write(self, message):
        if message.strip():
            self.log_queue.put(message)

    def flush(self):
        pass


class ETLGuiApp:
    """Aplicación principal de la interfaz gráfica del ETL"""

    def __init__(self, root):
        self.root = root
        self.root.title("ETL PostgreSQL - LADM-COL v2.0")
        self.root.geometry("900x750")
        self.root.minsize(800, 600)

        # Variables de configuración
        self.config_vars = {}
        self.init_config_vars()

        # Cola para logs
        self.log_queue = queue.Queue()

        # Flag para proceso en ejecución
        self.etl_running = False

        # Crear interfaz
        self.create_menu()
        self.create_main_interface()

        # Iniciar actualización de logs
        self.update_log_display()

        # Cargar última configuración si existe
        self.load_last_config()

    def init_config_vars(self):
        """Inicializa las variables de configuración"""
        # BD Origen
        self.config_vars['origen_host'] = tk.StringVar(value='localhost')
        self.config_vars['origen_port'] = tk.StringVar(value='5432')
        self.config_vars['origen_database'] = tk.StringVar(value='')
        self.config_vars['origen_user'] = tk.StringVar(value='postgres')
        self.config_vars['origen_password'] = tk.StringVar(value='')
        self.config_vars['origen_schema'] = tk.StringVar(value='')

        # BD Destino
        self.config_vars['destino_host'] = tk.StringVar(value='localhost')
        self.config_vars['destino_port'] = tk.StringVar(value='5432')
        self.config_vars['destino_database'] = tk.StringVar(value='')
        self.config_vars['destino_user'] = tk.StringVar(value='postgres')
        self.config_vars['destino_password'] = tk.StringVar(value='')
        self.config_vars['destino_schema'] = tk.StringVar(value='')

        # Rutas SQL
        self.config_vars['queries_path'] = tk.StringVar(value=str(BASE_DIR / 'sql' / 'queries'))
        self.config_vars['inserts_path'] = tk.StringVar(value=str(BASE_DIR / 'sql' / 'inserts'))
        self.config_vars['order_file'] = tk.StringVar(value=str(BASE_DIR / 'sql' / 'insert_order.txt'))
        self.config_vars['logs_path'] = tk.StringVar(value=str(BASE_DIR / 'logs'))

        # Opciones
        self.config_vars['batch_size'] = tk.StringVar(value='1000')
        self.config_vars['pool_size'] = tk.StringVar(value='5')
        self.config_vars['truncate_enabled'] = tk.BooleanVar(value=True)
        self.config_vars['truncate_cascade'] = tk.BooleanVar(value=True)
        self.config_vars['dry_run'] = tk.BooleanVar(value=False)

    def create_menu(self):
        """Crea el menú de la aplicación"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        # Menú Archivo
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Archivo", menu=file_menu)
        file_menu.add_command(label="Nueva configuración", command=self.new_config, accelerator="Ctrl+N")
        file_menu.add_command(label="Abrir configuración...", command=self.load_config, accelerator="Ctrl+O")
        file_menu.add_command(label="Guardar configuración", command=self.save_config, accelerator="Ctrl+S")
        file_menu.add_command(label="Guardar como...", command=self.save_config_as)
        file_menu.add_separator()
        file_menu.add_command(label="Salir", command=self.on_closing, accelerator="Alt+F4")

        # Menú Herramientas
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Herramientas", menu=tools_menu)
        tools_menu.add_command(label="Probar conexión origen", command=lambda: self.test_connection('origen'))
        tools_menu.add_command(label="Probar conexión destino", command=lambda: self.test_connection('destino'))
        tools_menu.add_separator()
        tools_menu.add_command(label="Limpiar logs", command=self.clear_logs)
        tools_menu.add_command(label="Abrir carpeta de logs", command=self.open_logs_folder)

        # Menú Ayuda
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Ayuda", menu=help_menu)
        help_menu.add_command(label="Acerca de...", command=self.show_about)

        # Atajos de teclado
        self.root.bind('<Control-n>', lambda e: self.new_config())
        self.root.bind('<Control-o>', lambda e: self.load_config())
        self.root.bind('<Control-s>', lambda e: self.save_config())

    def create_main_interface(self):
        """Crea la interfaz principal"""
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="5")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Notebook (pestañas)
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 5))

        # Crear pestañas
        self.create_origen_tab()
        self.create_destino_tab()
        self.create_rutas_tab()
        self.create_opciones_tab()

        # Frame para botones de acción
        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill=tk.X, pady=5)

        # Botones
        ttk.Button(action_frame, text="Probar Conexiones",
                   command=self.test_all_connections).pack(side=tk.LEFT, padx=5)

        self.run_button = ttk.Button(action_frame, text="▶ Ejecutar ETL",
                                      command=self.run_etl, style='Accent.TButton')
        self.run_button.pack(side=tk.LEFT, padx=5)

        self.stop_button = ttk.Button(action_frame, text="⬛ Detener",
                                       command=self.stop_etl, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        # Barra de progreso
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(action_frame, variable=self.progress_var,
                                             maximum=100, length=200)
        self.progress_bar.pack(side=tk.RIGHT, padx=5)

        self.status_label = ttk.Label(action_frame, text="Listo")
        self.status_label.pack(side=tk.RIGHT, padx=5)

        # Frame para logs
        log_frame = ttk.LabelFrame(main_frame, text="Visor de Logs", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True)

        # Área de texto para logs
        self.log_text = scrolledtext.ScrolledText(log_frame, height=12, wrap=tk.WORD,
                                                   font=('Consolas', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED)

        # Tags para colores en logs
        self.log_text.tag_config('error', foreground='red')
        self.log_text.tag_config('success', foreground='green')
        self.log_text.tag_config('warning', foreground='orange')
        self.log_text.tag_config('info', foreground='blue')

    def create_origen_tab(self):
        """Crea la pestaña de configuración de BD origen"""
        frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(frame, text="BD Origen")

        # Título
        ttk.Label(frame, text="Configuración de Base de Datos Origen",
                  font=('Segoe UI', 11, 'bold')).grid(row=0, column=0, columnspan=3, pady=(0, 15), sticky='w')

        # Campos
        fields = [
            ('Host:', 'origen_host', 'localhost'),
            ('Puerto:', 'origen_port', '5432'),
            ('Base de datos:', 'origen_database', ''),
            ('Usuario:', 'origen_user', 'postgres'),
            ('Contraseña:', 'origen_password', '', True),
            ('Esquema:', 'origen_schema', 'Esquema de origen (ej: cca_cun25436)'),
        ]

        for i, (label, var_name, placeholder, *args) in enumerate(fields):
            is_password = args[0] if args else False
            row = i + 1

            ttk.Label(frame, text=label).grid(row=row, column=0, sticky='e', padx=5, pady=5)

            entry = ttk.Entry(frame, textvariable=self.config_vars[var_name], width=40)
            if is_password:
                entry.config(show='*')
            entry.grid(row=row, column=1, sticky='w', padx=5, pady=5)

            if placeholder and not is_password:
                ttk.Label(frame, text=placeholder, foreground='gray').grid(row=row, column=2, sticky='w', padx=5)

        # Botón de prueba
        ttk.Button(frame, text="Probar conexión",
                   command=lambda: self.test_connection('origen')).grid(row=len(fields)+1, column=1, sticky='w', pady=10)

    def create_destino_tab(self):
        """Crea la pestaña de configuración de BD destino"""
        frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(frame, text="BD Destino")

        # Título
        ttk.Label(frame, text="Configuración de Base de Datos Destino",
                  font=('Segoe UI', 11, 'bold')).grid(row=0, column=0, columnspan=3, pady=(0, 15), sticky='w')

        # Campos
        fields = [
            ('Host:', 'destino_host', 'localhost'),
            ('Puerto:', 'destino_port', '5432'),
            ('Base de datos:', 'destino_database', ''),
            ('Usuario:', 'destino_user', 'postgres'),
            ('Contraseña:', 'destino_password', '', True),
            ('Esquema:', 'destino_schema', 'Esquema de destino (ej: cun25489)'),
        ]

        for i, (label, var_name, placeholder, *args) in enumerate(fields):
            is_password = args[0] if args else False
            row = i + 1

            ttk.Label(frame, text=label).grid(row=row, column=0, sticky='e', padx=5, pady=5)

            entry = ttk.Entry(frame, textvariable=self.config_vars[var_name], width=40)
            if is_password:
                entry.config(show='*')
            entry.grid(row=row, column=1, sticky='w', padx=5, pady=5)

            if placeholder and not is_password:
                ttk.Label(frame, text=placeholder, foreground='gray').grid(row=row, column=2, sticky='w', padx=5)

        # Botón de prueba
        ttk.Button(frame, text="Probar conexión",
                   command=lambda: self.test_connection('destino')).grid(row=len(fields)+1, column=1, sticky='w', pady=10)

    def create_rutas_tab(self):
        """Crea la pestaña de configuración de rutas"""
        frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(frame, text="Rutas SQL")

        # Título
        ttk.Label(frame, text="Configuración de Rutas de Archivos SQL",
                  font=('Segoe UI', 11, 'bold')).grid(row=0, column=0, columnspan=3, pady=(0, 15), sticky='w')

        # Rutas
        paths = [
            ('Carpeta de Queries:', 'queries_path', 'folder'),
            ('Carpeta de Inserts:', 'inserts_path', 'folder'),
            ('Archivo Order:', 'order_file', 'file'),
            ('Carpeta de Logs:', 'logs_path', 'folder'),
        ]

        for i, (label, var_name, path_type) in enumerate(paths):
            row = i + 1

            ttk.Label(frame, text=label).grid(row=row, column=0, sticky='e', padx=5, pady=8)

            entry = ttk.Entry(frame, textvariable=self.config_vars[var_name], width=55)
            entry.grid(row=row, column=1, sticky='w', padx=5, pady=8)

            if path_type == 'folder':
                btn = ttk.Button(frame, text="📁", width=3,
                                  command=lambda v=var_name: self.browse_folder(v))
            else:
                btn = ttk.Button(frame, text="📄", width=3,
                                  command=lambda v=var_name: self.browse_file(v))
            btn.grid(row=row, column=2, padx=5, pady=8)

        # Información adicional
        info_frame = ttk.LabelFrame(frame, text="Información", padding="10")
        info_frame.grid(row=len(paths)+1, column=0, columnspan=3, sticky='ew', pady=15)

        info_text = """• La carpeta de Queries contiene las consultas SELECT para extraer datos del origen.
• La carpeta de Inserts contiene las consultas INSERT para cargar datos al destino.
• El archivo Order define el orden de ejecución de los inserts (respetando dependencias).
• Los logs se guardan con timestamp en la carpeta especificada."""

        ttk.Label(info_frame, text=info_text, justify=tk.LEFT).pack(anchor='w')

    def create_opciones_tab(self):
        """Crea la pestaña de opciones"""
        frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(frame, text="Opciones")

        # Rendimiento
        perf_frame = ttk.LabelFrame(frame, text="Rendimiento", padding="10")
        perf_frame.pack(fill=tk.X, pady=5)

        ttk.Label(perf_frame, text="Tamaño de lote (batch):").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        ttk.Entry(perf_frame, textvariable=self.config_vars['batch_size'], width=10).grid(row=0, column=1, sticky='w', padx=5)
        ttk.Label(perf_frame, text="registros por transacción", foreground='gray').grid(row=0, column=2, sticky='w')

        ttk.Label(perf_frame, text="Pool de conexiones:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        ttk.Entry(perf_frame, textvariable=self.config_vars['pool_size'], width=10).grid(row=1, column=1, sticky='w', padx=5)
        ttk.Label(perf_frame, text="conexiones simultáneas", foreground='gray').grid(row=1, column=2, sticky='w')

        # TRUNCATE
        truncate_frame = ttk.LabelFrame(frame, text="TRUNCATE (Limpieza de tablas destino)", padding="10")
        truncate_frame.pack(fill=tk.X, pady=5)

        ttk.Checkbutton(truncate_frame, text="Habilitar TRUNCATE antes de insertar",
                        variable=self.config_vars['truncate_enabled']).pack(anchor='w', pady=2)
        ttk.Checkbutton(truncate_frame, text="Usar CASCADE (elimina dependencias)",
                        variable=self.config_vars['truncate_cascade']).pack(anchor='w', pady=2)

        # Modo ejecución
        exec_frame = ttk.LabelFrame(frame, text="Modo de Ejecución", padding="10")
        exec_frame.pack(fill=tk.X, pady=5)

        ttk.Checkbutton(exec_frame, text="Modo Dry-Run (simula sin hacer cambios reales)",
                        variable=self.config_vars['dry_run']).pack(anchor='w', pady=2)

        # Advertencia
        warn_frame = ttk.Frame(frame)
        warn_frame.pack(fill=tk.X, pady=10)

        ttk.Label(warn_frame, text="⚠️ ADVERTENCIA:", font=('Segoe UI', 9, 'bold'),
                  foreground='orange').pack(anchor='w')
        ttk.Label(warn_frame, text="El proceso ETL modificará datos en la base de datos destino. " +
                  "Asegúrese de tener un backup antes de ejecutar.",
                  wraplength=500, foreground='gray').pack(anchor='w')

    def browse_folder(self, var_name):
        """Abre diálogo para seleccionar carpeta"""
        current = self.config_vars[var_name].get()
        initial_dir = current if os.path.isdir(current) else str(BASE_DIR)

        folder = filedialog.askdirectory(initialdir=initial_dir, title="Seleccionar carpeta")
        if folder:
            self.config_vars[var_name].set(folder)

    def browse_file(self, var_name):
        """Abre diálogo para seleccionar archivo"""
        current = self.config_vars[var_name].get()
        initial_dir = os.path.dirname(current) if os.path.exists(os.path.dirname(current)) else str(BASE_DIR)

        file = filedialog.askopenfilename(
            initialdir=initial_dir,
            title="Seleccionar archivo",
            filetypes=[("Archivos de texto", "*.txt"), ("Todos los archivos", "*.*")]
        )
        if file:
            self.config_vars[var_name].set(file)

    def get_config_dict(self):
        """Obtiene la configuración actual como diccionario"""
        return {
            'origen': {
                'host': self.config_vars['origen_host'].get(),
                'port': self.config_vars['origen_port'].get(),
                'database': self.config_vars['origen_database'].get(),
                'user': self.config_vars['origen_user'].get(),
                'password': self.config_vars['origen_password'].get(),
                'schema': self.config_vars['origen_schema'].get(),
            },
            'destino': {
                'host': self.config_vars['destino_host'].get(),
                'port': self.config_vars['destino_port'].get(),
                'database': self.config_vars['destino_database'].get(),
                'user': self.config_vars['destino_user'].get(),
                'password': self.config_vars['destino_password'].get(),
                'schema': self.config_vars['destino_schema'].get(),
            },
            'paths': {
                'queries': self.config_vars['queries_path'].get(),
                'inserts': self.config_vars['inserts_path'].get(),
                'order_file': self.config_vars['order_file'].get(),
                'logs': self.config_vars['logs_path'].get(),
            },
            'options': {
                'batch_size': self.config_vars['batch_size'].get(),
                'pool_size': self.config_vars['pool_size'].get(),
                'truncate_enabled': self.config_vars['truncate_enabled'].get(),
                'truncate_cascade': self.config_vars['truncate_cascade'].get(),
                'dry_run': self.config_vars['dry_run'].get(),
            }
        }

    def set_config_from_dict(self, config):
        """Establece la configuración desde un diccionario"""
        if 'origen' in config:
            for key, value in config['origen'].items():
                var_name = f'origen_{key}'
                if var_name in self.config_vars:
                    self.config_vars[var_name].set(value)

        if 'destino' in config:
            for key, value in config['destino'].items():
                var_name = f'destino_{key}'
                if var_name in self.config_vars:
                    self.config_vars[var_name].set(value)

        if 'paths' in config:
            path_mapping = {
                'queries': 'queries_path',
                'inserts': 'inserts_path',
                'order_file': 'order_file',
                'logs': 'logs_path'
            }
            for key, value in config['paths'].items():
                var_name = path_mapping.get(key, key)
                if var_name in self.config_vars:
                    self.config_vars[var_name].set(value)

        if 'options' in config:
            option_mapping = {
                'batch_size': 'batch_size',
                'pool_size': 'pool_size',
                'truncate_enabled': 'truncate_enabled',
                'truncate_cascade': 'truncate_cascade',
                'dry_run': 'dry_run'
            }
            for key, value in config['options'].items():
                var_name = option_mapping.get(key, key)
                if var_name in self.config_vars:
                    self.config_vars[var_name].set(value)

    def new_config(self):
        """Crea una nueva configuración (valores por defecto)"""
        if messagebox.askyesno("Nueva configuración",
                               "¿Desea crear una nueva configuración? Se perderán los valores actuales."):
            self.init_config_vars()
            self.log_message("Nueva configuración creada", 'info')

    def save_config(self):
        """Guarda la configuración actual"""
        config_file = BASE_DIR / 'etl_config.json'
        self._save_config_to_file(str(config_file))

    def save_config_as(self):
        """Guarda la configuración en un archivo específico"""
        file = filedialog.asksaveasfilename(
            initialdir=str(BASE_DIR),
            title="Guardar configuración como",
            defaultextension=".json",
            filetypes=[("Archivos JSON", "*.json"), ("Todos los archivos", "*.*")]
        )
        if file:
            self._save_config_to_file(file)

    def _save_config_to_file(self, filepath):
        """Guarda la configuración en un archivo"""
        try:
            config = self.get_config_dict()
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            self.log_message(f"Configuración guardada en: {filepath}", 'success')
        except Exception as e:
            self.log_message(f"Error al guardar configuración: {e}", 'error')
            messagebox.showerror("Error", f"No se pudo guardar la configuración:\n{e}")

    def load_config(self):
        """Carga configuración desde archivo"""
        file = filedialog.askopenfilename(
            initialdir=str(BASE_DIR),
            title="Abrir configuración",
            filetypes=[("Archivos JSON", "*.json"), ("Todos los archivos", "*.*")]
        )
        if file:
            self._load_config_from_file(file)

    def _load_config_from_file(self, filepath):
        """Carga la configuración desde un archivo"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                config = json.load(f)
            self.set_config_from_dict(config)
            self.log_message(f"Configuración cargada desde: {filepath}", 'success')
        except Exception as e:
            self.log_message(f"Error al cargar configuración: {e}", 'error')

    def load_last_config(self):
        """Carga la última configuración guardada"""
        config_file = BASE_DIR / 'etl_config.json'
        if config_file.exists():
            self._load_config_from_file(str(config_file))

    def test_connection(self, db_type):
        """Prueba la conexión a una base de datos"""
        config = {
            'host': self.config_vars[f'{db_type}_host'].get(),
            'port': int(self.config_vars[f'{db_type}_port'].get()),
            'database': self.config_vars[f'{db_type}_database'].get(),
            'user': self.config_vars[f'{db_type}_user'].get(),
            'password': self.config_vars[f'{db_type}_password'].get(),
        }

        self.log_message(f"Probando conexión a BD {db_type}...", 'info')

        try:
            db_manager = DatabaseManager(config, pool_size=1)
            if db_manager.create_connection_pool():
                db_manager.close_pool()
                self.log_message(f"✅ Conexión exitosa a BD {db_type}", 'success')
                messagebox.showinfo("Conexión exitosa",
                                   f"La conexión a la base de datos {db_type} fue exitosa.")
            else:
                self.log_message(f"❌ Error de conexión a BD {db_type}", 'error')
                messagebox.showerror("Error de conexión",
                                    f"No se pudo conectar a la base de datos {db_type}.")
        except Exception as e:
            self.log_message(f"❌ Error: {e}", 'error')
            messagebox.showerror("Error de conexión", str(e))

    def test_all_connections(self):
        """Prueba todas las conexiones"""
        self.test_connection('origen')
        self.test_connection('destino')

    def log_message(self, message, level='info'):
        """Agrega un mensaje al log"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        full_message = f"[{timestamp}] {message}\n"
        self.log_queue.put((full_message, level))

    def update_log_display(self):
        """Actualiza el display de logs desde la cola"""
        try:
            while True:
                item = self.log_queue.get_nowait()
                if isinstance(item, tuple):
                    message, level = item
                else:
                    message, level = item, 'info'

                self.log_text.config(state=tk.NORMAL)
                self.log_text.insert(tk.END, message, level)
                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)
        except queue.Empty:
            pass

        # Programar siguiente actualización
        self.root.after(100, self.update_log_display)

    def clear_logs(self):
        """Limpia el área de logs"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)

    def open_logs_folder(self):
        """Abre la carpeta de logs en el explorador"""
        logs_path = self.config_vars['logs_path'].get()
        if os.path.exists(logs_path):
            os.startfile(logs_path)
        else:
            messagebox.showwarning("Carpeta no encontrada",
                                  f"La carpeta de logs no existe:\n{logs_path}")

    def validate_config(self):
        """Valida la configuración antes de ejecutar"""
        errors = []

        # Validar conexiones
        if not self.config_vars['origen_database'].get():
            errors.append("Falta la base de datos de origen")
        if not self.config_vars['origen_password'].get():
            errors.append("Falta la contraseña de origen")
        if not self.config_vars['destino_database'].get():
            errors.append("Falta la base de datos de destino")
        if not self.config_vars['destino_password'].get():
            errors.append("Falta la contraseña de destino")

        # Validar rutas
        queries_path = self.config_vars['queries_path'].get()
        if not os.path.isdir(queries_path):
            errors.append(f"La carpeta de queries no existe: {queries_path}")

        inserts_path = self.config_vars['inserts_path'].get()
        if not os.path.isdir(inserts_path):
            errors.append(f"La carpeta de inserts no existe: {inserts_path}")

        order_file = self.config_vars['order_file'].get()
        if not os.path.isfile(order_file):
            errors.append(f"El archivo de orden no existe: {order_file}")

        # Validar esquemas
        if not self.config_vars['origen_schema'].get():
            errors.append("Falta el esquema de origen")
        if not self.config_vars['destino_schema'].get():
            errors.append("Falta el esquema de destino")

        return errors

    def run_etl(self):
        """Ejecuta el proceso ETL"""
        # Validar configuración
        errors = self.validate_config()
        if errors:
            error_msg = "Por favor corrija los siguientes errores:\n\n• " + "\n• ".join(errors)
            messagebox.showerror("Errores de configuración", error_msg)
            return

        # Confirmar ejecución
        if not self.config_vars['dry_run'].get():
            if not messagebox.askyesno("Confirmar ejecución",
                                       "¿Está seguro de ejecutar el proceso ETL?\n\n" +
                                       "Esto modificará datos en la base de datos destino."):
                return

        # Deshabilitar botones
        self.etl_running = True
        self.run_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.status_label.config(text="Ejecutando...")
        self.progress_var.set(0)

        # Ejecutar en hilo separado
        thread = threading.Thread(target=self._run_etl_thread, daemon=True)
        thread.start()

    def _run_etl_thread(self):
        """Ejecuta el ETL en un hilo separado"""
        try:
            self.log_message("=" * 60)
            self.log_message("INICIANDO PROCESO ETL", 'info')
            self.log_message("=" * 60)

            # Configuración de BD origen
            origen_config = {
                'host': self.config_vars['origen_host'].get(),
                'port': int(self.config_vars['origen_port'].get()),
                'database': self.config_vars['origen_database'].get(),
                'user': self.config_vars['origen_user'].get(),
                'password': self.config_vars['origen_password'].get(),
            }

            # Configuración de BD destino
            destino_config = {
                'host': self.config_vars['destino_host'].get(),
                'port': int(self.config_vars['destino_port'].get()),
                'database': self.config_vars['destino_database'].get(),
                'user': self.config_vars['destino_user'].get(),
                'password': self.config_vars['destino_password'].get(),
            }

            # Rutas
            paths = {
                'queries': self.config_vars['queries_path'].get(),
                'inserts': self.config_vars['inserts_path'].get(),
                'logs': self.config_vars['logs_path'].get(),
                'order_file': self.config_vars['order_file'].get(),
            }

            # Rendimiento
            performance = {
                'batch_size': int(self.config_vars['batch_size'].get()),
                'connection_pool_size': int(self.config_vars['pool_size'].get()),
                'max_retries': 3,
                'timeout': 300
            }

            # TRUNCATE
            truncate_config = {
                'enabled': self.config_vars['truncate_enabled'].get() and not self.config_vars['dry_run'].get(),
                'require_confirmation': False,  # Ya confirmamos en la GUI
                'use_cascade': self.config_vars['truncate_cascade'].get(),
                'skip_if_not_exists': True
            }

            # Esquemas
            schemas = [self.config_vars['destino_schema'].get()]

            self.log_message(f"BD Origen: {origen_config['host']}:{origen_config['port']}/{origen_config['database']}")
            self.log_message(f"BD Destino: {destino_config['host']}:{destino_config['port']}/{destino_config['database']}")
            self.log_message(f"Esquema origen: {self.config_vars['origen_schema'].get()}")
            self.log_message(f"Esquema destino: {self.config_vars['destino_schema'].get()}")
            self.log_message(f"Modo Dry-Run: {self.config_vars['dry_run'].get()}")

            self.progress_var.set(10)

            # Crear procesador ETL
            etl = ETLProcessor(
                origen_config=origen_config,
                destino_config=destino_config,
                paths=paths,
                performance_config=performance,
                truncate_config=truncate_config
            )

            self.progress_var.set(20)
            self.log_message("Conectando a bases de datos...", 'info')

            # Ejecutar ETL
            success = etl.run_etl(schemas)

            self.progress_var.set(100)

            if success:
                self.log_message("=" * 60)
                self.log_message("✅ PROCESO ETL COMPLETADO EXITOSAMENTE", 'success')
                self.log_message("=" * 60)
                self.root.after(0, lambda: messagebox.showinfo("Éxito", "El proceso ETL se completó exitosamente."))
            else:
                self.log_message("=" * 60)
                self.log_message("❌ PROCESO ETL COMPLETADO CON ERRORES", 'error')
                self.log_message("=" * 60)
                self.root.after(0, lambda: messagebox.showwarning("Advertencia",
                                                                   "El proceso ETL terminó con errores. Revise los logs."))

        except Exception as e:
            self.log_message(f"❌ Error crítico: {e}", 'error')
            self.root.after(0, lambda: messagebox.showerror("Error", f"Error durante la ejecución:\n{e}"))

        finally:
            # Rehabilitar botones
            self.etl_running = False
            self.root.after(0, self._reset_ui_after_etl)

    def _reset_ui_after_etl(self):
        """Resetea la UI después de ejecutar ETL"""
        self.run_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.status_label.config(text="Listo")

    def stop_etl(self):
        """Detiene el proceso ETL"""
        if self.etl_running:
            if messagebox.askyesno("Confirmar", "¿Desea detener el proceso ETL?"):
                self.etl_running = False
                self.log_message("⚠️ Proceso ETL detenido por el usuario", 'warning')
                self._reset_ui_after_etl()

    def show_about(self):
        """Muestra información sobre la aplicación"""
        about_text = """ETL PostgreSQL - LADM-COL

Versión: 2.0.0
Interfaz gráfica para migración de datos catastrales

Características:
• Migración de datos entre bases PostgreSQL
• Configuración flexible de rutas y conexiones
• Visor de logs en tiempo real
• Guardado de configuraciones

Desarrollado para el proyecto LADM-COL"""

        messagebox.showinfo("Acerca de", about_text)

    def on_closing(self):
        """Maneja el cierre de la aplicación"""
        if self.etl_running:
            if not messagebox.askyesno("Confirmar cierre",
                                       "Hay un proceso ETL en ejecución. ¿Desea cerrar de todos modos?"):
                return

        # Guardar configuración actual
        try:
            self.save_config()
        except:
            pass

        self.root.destroy()


def main():
    """Función principal"""
    root = tk.Tk()

    # Configurar estilo
    style = ttk.Style()
    style.theme_use('clam')  # Tema más moderno

    # Estilo personalizado para botón de acción
    style.configure('Accent.TButton', font=('Segoe UI', 10, 'bold'))

    # Crear aplicación
    app = ETLGuiApp(root)

    # Manejar cierre
    root.protocol("WM_DELETE_WINDOW", app.on_closing)

    # Iniciar loop principal
    root.mainloop()


if __name__ == "__main__":
    main()
