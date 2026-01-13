# 📘 Guía de Uso: Funcionalidad TRUNCATE

## 🎯 Descripción

El sistema ETL ahora incluye funcionalidad automática de TRUNCATE que limpia las tablas destino antes de insertar nuevos datos, evitando duplicados y asegurando que cada migración parta de un estado limpio.

---

## ⚙️ Configuración

La configuración de TRUNCATE se encuentra en `config/config.py`:

```python
TRUNCATE_CONFIG = {
    'enabled': True,             # True = ejecuta TRUNCATE, False = omite TRUNCATE
    'require_confirmation': True, # True = pide confirmación, False = ejecuta automáticamente
    'use_cascade': True          # True = usa TRUNCATE CASCADE, False = TRUNCATE simple
}
```

### Opciones de Configuración:

#### `enabled` (bool)
- **`True`**: El TRUNCATE se ejecutará (recomendado)
- **`False`**: El TRUNCATE se omitirá completamente

**Cuándo usar `False`:**
- Cuando quieres hacer inserts incrementales sin borrar datos existentes
- Para pruebas donde no quieres limpiar las tablas

#### `require_confirmation` (bool)
- **`True`**: El proceso pedirá confirmación antes de ejecutar TRUNCATE (recomendado)
- **`False`**: El TRUNCATE se ejecutará automáticamente sin pedir confirmación

**Cuándo usar `False`:**
- Ejecuciones automatizadas (cron jobs, scripts)
- Ambientes de desarrollo/prueba donde no hay riesgo
- Cuando estás seguro y quieres agilizar el proceso

⚠️ **PRECAUCIÓN**: Usar `False` en producción es PELIGROSO

#### `use_cascade` (bool)
- **`True`**: Usa `TRUNCATE CASCADE` (recomendado)
- **`False`**: Usa `TRUNCATE` simple

**Diferencias:**
- **CASCADE**: Trunca también las tablas con llaves foráneas dependientes
- **Simple**: Fallará si hay dependencias de llaves foráneas

---

## 📋 Escenarios de Uso

### 1️⃣ Producción (Máxima Seguridad)
```python
TRUNCATE_CONFIG = {
    'enabled': True,
    'require_confirmation': True,  # ✅ Pide confirmación
    'use_cascade': True
}
```
**Resultado:** Pedirá "SI" antes de truncar. Más seguro.

---

### 2️⃣ Desarrollo/Pruebas (Modo Rápido)
```python
TRUNCATE_CONFIG = {
    'enabled': True,
    'require_confirmation': False,  # ⚡ Sin confirmación
    'use_cascade': True
}
```
**Resultado:** Ejecuta TRUNCATE automáticamente. Más rápido.

---

### 3️⃣ Inserts Incrementales (Sin TRUNCATE)
```python
TRUNCATE_CONFIG = {
    'enabled': False,  # ❌ No trunca
    'require_confirmation': True,
    'use_cascade': True
}
```
**Resultado:** No borra datos, solo inserta. **Puede causar duplicados**.

---

### 4️⃣ Automatización/Scripts (Sin Interacción)
```python
TRUNCATE_CONFIG = {
    'enabled': True,
    'require_confirmation': False,  # 🤖 Automatizado
    'use_cascade': True
}
```
**Resultado:** Ideal para cron jobs o ejecuciones en background.

---

## 🚀 Ejemplos de Ejecución

### Ejecución Interactiva (con confirmación):

```bash
python main.py
```

**Salida esperada:**
```
================================================================================
⚠️  ADVERTENCIA: LIMPIEZA DE TABLAS
================================================================================
📋 Se truncarán 29 tabla(s) en el esquema:
   🗑️  cun25436.cc_barrio
   🗑️  cun25436.cc_centropoblado
   ... (27 tablas más)

⚠️  IMPORTANTE: Se recomienda tener un backup antes de continuar
⚠️  Esta operación eliminará TODOS los datos de estas tablas
⚠️  Se usará TRUNCATE CASCADE (afectará tablas dependientes)
================================================================================

¿Desea continuar con el TRUNCATE? (escriba 'SI' para confirmar): _
```

Debes escribir **`SI`** (en mayúsculas) y presionar Enter.

---

### Ejecución Automatizada (sin confirmación):

1. Cambiar configuración:
```python
TRUNCATE_CONFIG = {
    'enabled': True,
    'require_confirmation': False,  # ← Cambiar a False
    'use_cascade': True
}
```

2. Ejecutar:
```bash
python main.py
```

**Salida esperada:**
```
================================================================================
⚠️  ADVERTENCIA: LIMPIEZA DE TABLAS
================================================================================
📋 Se truncarán 29 tabla(s) en el esquema:
   ... (lista de tablas) ...
⚠️  Ejecutando TRUNCATE automáticamente (sin confirmación)
🧹 Iniciando limpieza de tablas con TRUNCATE CASCADE...
🧹 Tabla cun25436.cc_barrio truncada exitosamente
...
✅ Limpieza completada exitosamente: 29 tabla(s) truncada(s)
```

---

## 🔍 Verificación

### Tablas Identificadas

El sistema analiza automáticamente todos los archivos SQL en `sql/inserts/` y extrae las tablas destino.

**Ver qué tablas se truncarán:**
```bash
python test_truncate.py
```

**Salida:**
```
✅ Se identificaron 29 tablas destino:
   🗑️  cun25436.cc_barrio
   🗑️  cun25436.cc_centropoblado
   ... (lista completa)
```

---

## ⚠️ Recomendaciones Importantes

### 1. **Siempre hacer backup antes de ejecutar en producción**
```bash
# Backup de la base de datos completa
pg_dump -h localhost -p 5433 -U postgres -d interno_gc_acc \
  -F c -f backup_$(date +%Y%m%d_%H%M%S).backup

# Backup de un esquema específico
pg_dump -h localhost -p 5433 -U postgres -d interno_gc_acc \
  -n cun25436 -F c -f backup_cun25436_$(date +%Y%m%d_%H%M%S).backup
```

### 2. **Probar primero en ambiente de desarrollo**
- Nunca ejecutar directamente en producción sin probar
- Verificar que el TRUNCATE funciona correctamente
- Validar los conteos de registros después

### 3. **Usar confirmación en producción**
```python
'require_confirmation': True  # ✅ SIEMPRE en producción
```

### 4. **Revisar los logs**
Todos los TRUNCATE quedan registrados en:
```
logs/etl_execution_YYYYMMDD_HHMMSS.txt
```

### 5. **Validar después de la migración**
```sql
-- Verificar conteos
SELECT schemaname, tablename, n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'cun25436'
ORDER BY tablename;
```

---

## 🆘 Troubleshooting

### Problema: "EOF when reading a line"

**Causa:** El proceso se está ejecutando en modo no interactivo (background) pero `require_confirmation` está en `True`.

**Solución:**
```python
# Opción 1: Cambiar configuración
TRUNCATE_CONFIG = {
    'enabled': True,
    'require_confirmation': False,  # ← Cambiar aquí
    'use_cascade': True
}

# Opción 2: Ejecutar en modo interactivo (no en background)
python main.py  # Sin redirecciones
```

---

### Problema: "permission denied for table"

**Causa:** El usuario de PostgreSQL no tiene permisos de TRUNCATE.

**Solución:**
```sql
-- Dar permisos de TRUNCATE al usuario
GRANT TRUNCATE ON ALL TABLES IN SCHEMA cun25436 TO postgres;
```

---

### Problema: TRUNCATE falla con "cannot truncate a table referenced in a foreign key constraint"

**Causa:** Hay llaves foráneas y no estás usando CASCADE.

**Solución:**
```python
TRUNCATE_CONFIG = {
    'enabled': True,
    'require_confirmation': True,
    'use_cascade': True  # ← Debe ser True
}
```

---

### Problema: No quiero truncar ciertas tablas

**Solución Actual:** El sistema trunca TODAS las tablas destino identificadas en los INSERT.

**Solución Temporal:** Comentar los archivos INSERT de las tablas que no quieres truncar en `sql/insert_order.txt`.

**Mejora Futura:** Agregar lista de exclusión en config:
```python
TRUNCATE_CONFIG = {
    'enabled': True,
    'require_confirmation': True,
    'use_cascade': True,
    'exclude_tables': ['cun25436.gc_interesado']  # No implementado aún
}
```

---

## 📊 Diagrama de Flujo

```
┌─────────────────────────────────────┐
│   Iniciar Proceso ETL               │
└──────────────┬──────────────────────┘
               │
               ▼
      ┌────────────────┐
      │ enabled=True?  │
      └────┬───────┬───┘
           │       │
          SI      NO
           │       │
           │       └──────────────┐
           ▼                      │
┌──────────────────────┐          │
│ Analizar archivos    │          │
│ INSERT → Extraer     │          │
│ tablas destino       │          │
└──────────┬───────────┘          │
           │                      │
           ▼                      │
┌──────────────────────┐          │
│ Mostrar advertencia  │          │
└──────────┬───────────┘          │
           │                      │
           ▼                      │
   ┌────────────────────┐         │
   │require_confirmation?│        │
   └───┬────────────┬───┘         │
       │            │             │
      SI           NO             │
       │            │             │
       ▼            ▼             │
┌────────────┐ ┌────────────┐    │
│Pedir 'SI'  │ │Auto-ejecutar│   │
└──┬──────┬──┘ └─────┬──────┘    │
   │      │          │            │
  SI     NO          │            │
   │      │          │            │
   │      └──────────┼────────────┤
   │                 │            │
   ▼                 ▼            ▼
┌────────────────────────────────────┐
│      TRUNCATE CASCADE/SIMPLE       │
└──────────────┬─────────────────────┘
               │
               ▼
┌────────────────────────────────────┐
│  Continuar con Queries + INSERTs   │
└────────────────────────────────────┘
```

---

## 📝 Resumen

| Configuración | Producción | Desarrollo | Automatización |
|--------------|------------|------------|----------------|
| `enabled` | ✅ True | ✅ True | ✅ True |
| `require_confirmation` | ✅ True | ❌ False | ❌ False |
| `use_cascade` | ✅ True | ✅ True | ✅ True |

**Regla de oro:** Siempre hacer backup antes de ejecutar en producción.

---

## 🔗 Archivos Relacionados

- **Configuración:** `config/config.py`
- **Implementación:** `src/etl_processor.py`
- **Database Manager:** `src/database_manager.py`
- **Script de prueba:** `test_truncate.py`
- **Recomendaciones:** `RECOMENDACIONES.md`
