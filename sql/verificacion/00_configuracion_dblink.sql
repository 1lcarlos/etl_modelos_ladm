-- ============================================================================
-- ARCHIVO 00: Configuracion dblink
-- ============================================================================
-- Ejecutar desde: pgAdmin conectado a base DESTINO (puerto 5432, db actualizacion)
-- Proposito: Configurar dblink para acceder a base ORIGEN (puerto 5433, db ladm_col)
-- Fecha: 2026-02-08
-- ============================================================================

-- 1. Crear extension dblink si no existe
CREATE EXTENSION IF NOT EXISTS dblink;

-- 2. Constante de conexion al ORIGEN
-- Usar esta cadena en todas las consultas dblink de los archivos 01-05
-- Conexion: host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123

-- 3. Test de conectividad: Leer cca_predio desde destino via dblink
SELECT *
FROM dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    'SELECT count(*) FROM cun25436.cca_predio'
) AS t(total_registros_cca_predio bigint);

-- 4. Test de lectura de tabla destino
SELECT count(*) AS total_registros_gc_predio
FROM cun25436.gc_predio;

-- 5. Resumen de conectividad
SELECT
    'ORIGEN (CCA - puerto 5433)' AS base,
    (SELECT *
     FROM dblink(
         'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
         'SELECT count(*)::text FROM cun25436.cca_predio'
     ) AS t(cnt text)) AS registros_cca_predio,
    'OK' AS estado
UNION ALL
SELECT
    'DESTINO (Django - puerto 5432)' AS base,
    count(*)::text AS registros_gc_predio,
    'OK' AS estado
FROM cun25436.gc_predio;
