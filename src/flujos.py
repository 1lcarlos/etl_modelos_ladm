# src/flujos.py
"""
Flujos de migracion: cada flujo es una carpeta de queries, una carpeta de inserts
y un archivo de orden. Estas funciones no usan la base de datos, asi que sirven para
verificar un flujo antes de ejecutarlo (GUI) y para que el procesador lea el orden.
"""

import json
import os
import re
from typing import Dict, List, Optional, Set, Tuple

# Prefijos en el archivo de orden: '#' comentario, '-' archivo desactivado
PREFIJO_COMENTARIO = '#'
PREFIJO_DESACTIVADO = '-'

# INSERT INTO esquema.tabla o INSERT INTO tabla. Los nombres entre comillas no se capturan,
# asi las tablas de control como "etl_control"."incidencia" no se truncan.
PATRON_INSERT = re.compile(
    r'INSERT\s+INTO\s+(?:([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)|([a-zA-Z_][a-zA-Z0-9_]*)(?!\.))',
    re.IGNORECASE
)

# Referencias a tablas temporales creadas desde las queries (tmp_<archivo>)
PATRON_TMP = re.compile(r'\btmp_\w+', re.IGNORECASE)
PATRON_TMP_CREADA = re.compile(
    r'\bcreate\s+(?:(?:global\s+|local\s+)?(?:temp|temporary)\s+)?(?:unlogged\s+)?table\s+'
    r'(?:if\s+not\s+exists\s+)?(tmp_\w+)',
    re.IGNORECASE
)

# Control de transacciones a nivel de sentencia. Se busca fuera de comentarios y de cuerpos $$
# (los END; de PL/pgSQL no cuentan). Un COMMIT dentro de un archivo romperia la simulacion.
PATRON_TRANSACCION = re.compile(
    r'(?:^|;)\s*(COMMIT\b|ROLLBACK\b(?!\s+TO\b)|BEGIN\s*;|BEGIN\s+(?:TRANSACTION|WORK)\b|START\s+TRANSACTION\b)',
    re.IGNORECASE
)


def quitar_comentarios(sql: str) -> str:
    """Elimina comentarios SQL de bloque (/* */) y de linea (--)"""
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    return re.sub(r'--[^\n]*', ' ', sql)


def _quitar_cuerpos_dolar(sql: str) -> str:
    """Elimina los cuerpos $$...$$ / $tag$...$tag$ (funciones y bloques DO)"""
    return re.sub(r'(\$[A-Za-z0-9_]*\$).*?\1', ' ', sql, flags=re.DOTALL)


def leer_orden(order_file: str) -> Tuple[List[str], List[str]]:
    """
    Lee el archivo de orden de inserts

    Args:
        order_file: Ruta del archivo de orden

    Returns:
        (activos, desactivados): archivos a ejecutar en orden y archivos desactivados con '-'

    Raises:
        FileNotFoundError: Si el archivo no existe (o la ruta esta vacia)
    """
    activos, desactivados = [], []
    with open(order_file, 'r', encoding='utf-8-sig') as f:
        for linea in f:
            linea = linea.strip()
            if not linea or linea.startswith(PREFIJO_COMENTARIO):
                continue
            if linea.startswith(PREFIJO_DESACTIVADO):
                desactivados.append(linea.lstrip(PREFIJO_DESACTIVADO).strip())
            else:
                activos.append(linea)
    return activos, desactivados


def listar_sql(directorio: str) -> List[str]:
    """Nombres de los archivos .sql de un directorio, en orden alfabetico"""
    if not directorio or not os.path.isdir(directorio):
        return []
    return sorted(
        nombre for nombre in os.listdir(directorio)
        if nombre.lower().endswith('.sql') and os.path.isfile(os.path.join(directorio, nombre))
    )


def leer_sql(ruta: str) -> str:
    """Lee un archivo SQL tolerando caracteres invalidos"""
    with open(ruta, 'r', encoding='utf-8', errors='replace') as f:
        return f.read()


def tablas_destino(sql: str, schema: str) -> Set[str]:
    """
    Tablas destino de los INSERT INTO de un archivo (las que se truncan antes de cargar)

    Args:
        sql: Contenido del archivo (con el placeholder {schema})
        schema: Esquema que reemplaza al placeholder

    Returns:
        Set[str]: Tablas con el esquema incluido
    """
    tablas = set()
    for con_esquema, sin_esquema in PATRON_INSERT.findall(sql.replace('{schema}', schema)):
        tabla = con_esquema or sin_esquema
        if 't_ili2db' in tabla.lower():
            continue  # Secuencias ili2db
        tablas.add(tabla if '.' in tabla else f"{schema}.{tabla}")
    return tablas


def tiene_control_transaccion(sql: str) -> bool:
    """True si el archivo contiene BEGIN/COMMIT/ROLLBACK a nivel de sentencia"""
    return bool(PATRON_TRANSACCION.search(_quitar_cuerpos_dolar(quitar_comentarios(sql))))


def verificar_flujo(queries_dir: str, inserts_dir: str, order_file: str, schema: str = 'esquema') -> Dict:
    """
    Revisa un flujo sin conectarse a la base de datos

    Args:
        queries_dir: Carpeta de queries (cada archivo crea la tabla tmp_<archivo>)
        inserts_dir: Carpeta de inserts
        order_file: Archivo de orden; vacio = orden alfabetico de la carpeta de inserts
        schema: Esquema usado para listar las tablas que se truncaran

    Returns:
        Dict con queries, inserts (archivo/estado/detalle), desactivados, sin_orden,
        tablas_destino, orden_alfabetico, errores y advertencias
    """
    resultado = {
        'queries': [], 'inserts': [], 'desactivados': [], 'sin_orden': [],
        'tablas_destino': [], 'orden_alfabetico': False, 'errores': [], 'advertencias': [],
    }
    errores, advertencias = resultado['errores'], resultado['advertencias']

    queries = listar_sql(queries_dir)
    if not queries_dir or not os.path.isdir(queries_dir):
        errores.append(f"No existe la carpeta de queries: {queries_dir or '(sin definir)'}")
    elif not queries:
        errores.append(f"La carpeta de queries no tiene archivos .sql: {queries_dir}")

    archivos_insert = listar_sql(inserts_dir)
    if not inserts_dir or not os.path.isdir(inserts_dir):
        errores.append(f"No existe la carpeta de inserts: {inserts_dir or '(sin definir)'}")
    elif not archivos_insert:
        errores.append(f"La carpeta de inserts no tiene archivos .sql: {inserts_dir}")

    if order_file:
        if not os.path.isfile(order_file):
            errores.append(f"No existe el archivo de orden: {order_file}")
            activos, desactivados = [], []
        else:
            activos, desactivados = leer_orden(order_file)
            if not activos:
                errores.append(f"El archivo de orden no tiene archivos activos: {order_file}")
    else:
        activos, desactivados = list(archivos_insert), []
        resultado['orden_alfabetico'] = True
        advertencias.append("Sin archivo de orden: los inserts se ejecutan en orden alfabetico")

    resultado['queries'] = queries
    if errores:
        return resultado

    tmp_disponibles = {f"tmp_{os.path.splitext(q)[0].lower()}" for q in queries}
    contenidos = {}
    for nombre in activos:
        ruta = os.path.join(inserts_dir, nombre)
        if os.path.isfile(ruta):
            contenidos[nombre] = leer_sql(ruta)

    # Tablas tmp_ que crean los propios inserts (pueden usarse en archivos posteriores)
    tmp_creadas = set()
    for sql in contenidos.values():
        tmp_creadas |= {t.lower() for t in PATRON_TMP_CREADA.findall(quitar_comentarios(sql))}

    tablas = set()
    for nombre in activos:
        item = {'archivo': nombre, 'estado': 'ok', 'detalle': ''}
        if nombre not in contenidos:
            item['estado'] = 'faltante'
            item['detalle'] = 'No existe en la carpeta de inserts (se omitira)'
            advertencias.append(f"{nombre}: no existe en la carpeta de inserts (se omitira)")
        else:
            sql = contenidos[nombre]
            tablas |= tablas_destino(sql, schema)
            problemas = []
            usadas = {t.lower() for t in PATRON_TMP.findall(quitar_comentarios(sql))}
            faltan = sorted(usadas - tmp_disponibles - tmp_creadas)
            if faltan:
                problemas.append("usa tablas sin query: " + ", ".join(faltan))
            if tiene_control_transaccion(sql):
                problemas.append("contiene BEGIN/COMMIT/ROLLBACK (no se puede simular)")
            if problemas:
                item['estado'] = 'advertencia'
                item['detalle'] = '; '.join(problemas)
                advertencias.append(f"{nombre}: {item['detalle']}")
        resultado['inserts'].append(item)

    resultado['desactivados'] = desactivados
    resultado['sin_orden'] = sorted(set(archivos_insert) - set(activos) - set(desactivados))
    resultado['tablas_destino'] = sorted(tablas)
    return resultado


def cargar_flujos(ruta_json: str, base_dir: str) -> List[Dict]:
    """
    Lee la lista de flujos (config/flujos.json)

    Args:
        ruta_json: Ruta del archivo JSON
        base_dir: Directorio contra el que se resuelven las rutas relativas

    Returns:
        List[Dict]: Flujos con id, nombre, descripcion, vigente y rutas absolutas
    """
    with open(ruta_json, 'r', encoding='utf-8-sig') as f:
        datos = json.load(f)

    def ruta(valor):
        return os.path.normpath(os.path.join(str(base_dir), valor)) if valor else ''

    flujos = []
    for item in datos.get('flujos', []):
        flujos.append({
            'id': item['id'],
            'nombre': item.get('nombre', item['id']),
            'descripcion': item.get('descripcion', ''),
            'vigente': item.get('vigente', True),
            'queries': ruta(item.get('queries', '')),
            'inserts': ruta(item.get('inserts', '')),
            'orden': ruta(item.get('orden', '')),
        })
    return flujos


def _misma_ruta(a: str, b: str) -> bool:
    if not a or not b:
        return not a and not b
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def identificar_flujo(flujos: List[Dict], queries: str, inserts: str, orden: str) -> Optional[Dict]:
    """Devuelve el flujo cuyas rutas coinciden con las dadas, o None si son rutas propias"""
    for flujo in flujos:
        if (_misma_ruta(flujo['queries'], queries) and _misma_ruta(flujo['inserts'], inserts)
                and _misma_ruta(flujo['orden'], orden)):
            return flujo
    return None
