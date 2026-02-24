-- ============================================================================
-- UPDATE area_registral_m2 en gc_predio (destino v3)
-- desde cca_predio (origen CCA)
-- ============================================================================
--
-- Descripcion: Actualiza el campo area_registral_m2 en la tabla gc_predio
--              del modelo destino (v3), usando los datos de
--              area_registral_m2 de la tabla cca_predio del modelo CCA origen.
--
-- Ejecutar en: Base de datos DESTINO (interno_v3_acc, port 5433)
--
-- Origen (CCA): host=localhost port=5433 dbname=ladm_col
--               Schema: cca_cun25436
--               Tabla: cca_predio (campos: numero_predial, nupre, area_registral_m2)
--
-- Destino (v3): Schema configurable via {schema}
--               Tabla: gc_predio (campos: numero_predial_nacional, nupre, area_registral_m2)
--
-- Join: numero_predial (CCA) = numero_predial_nacional (destino)
--       nupre (CCA) = nupre (destino)
--
-- IMPORTANTE:
--   1. Reemplazar {schema} con el schema destino (ej: cun25436)
--   2. Ajustar la cadena de conexion dblink si la BD CCA esta en otro servidor/puerto
--   3. Ajustar cca_cun25436 si el schema CCA tiene otro nombre
--   4. Se recomienda ejecutar los pasos de verificacion (PASO 2) antes del UPDATE (PASO 3)
--
-- Fecha: 2026-02-11
-- ============================================================================

-- Verificar que la extension dblink este habilitada
CREATE EXTENSION IF NOT EXISTS dblink;

-- ============================================================================
-- PASO 1: Crear tabla temporal con datos del origen CCA via dblink
-- ============================================================================
-- NOTA: Ajustar la cadena de conexion segun donde este restaurada la BD CCA
--   - host: servidor donde esta la BD CCA
--   - port: puerto de PostgreSQL (5433 = ladm_col)
--   - dbname: nombre de la BD CCA (ladm_col)
--   - user/password: credenciales

DROP TABLE IF EXISTS tmp_update_area_registral;

CREATE TEMP TABLE tmp_update_area_registral AS
SELECT * FROM dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$
    SELECT DISTINCT ON (p.numero_predial)
        p.numero_predial,
        p.nupre,
        p.area_registral_m2
    FROM cca_cun25436.cca_predio p
    WHERE p.area_registral_m2 IS NOT NULL
      AND p.numero_predial IS NOT NULL
    ORDER BY p.numero_predial, p.t_id
    $$
) AS t(
    numero_predial varchar(30),
    nupre varchar(11),
    area_registral_m2 numeric(25,2)
);

-- ============================================================================
-- PASO 2: Verificacion previa (ejecutar ANTES del UPDATE para confirmar datos)
-- ============================================================================

-- 2.1 Total de registros extraidos del origen CCA
SELECT 'Registros extraidos del origen CCA' AS descripcion,
       count(*) AS total
FROM tmp_update_area_registral;

-- 2.2 Registros que cruzan por numero_predial Y nupre (match exacto)
SELECT 'Cruzan por numero_predial Y nupre' AS descripcion,
       count(*) AS total
FROM {schema}.gc_predio gp
INNER JOIN tmp_update_area_registral tmp
    ON gp.numero_predial_nacional = tmp.numero_predial
    AND gp.nupre = tmp.nupre;

-- 2.3 Registros que cruzan SOLO por numero_predial (sin considerar nupre)
SELECT 'Cruzan solo por numero_predial' AS descripcion,
       count(*) AS total
FROM {schema}.gc_predio gp
INNER JOIN tmp_update_area_registral tmp
    ON gp.numero_predial_nacional = tmp.numero_predial;

-- 2.4 Registros destino que actualmente tienen area_registral_m2 NULL
SELECT 'Registros destino con area_registral_m2 NULL' AS descripcion,
       count(*) AS total
FROM {schema}.gc_predio
WHERE area_registral_m2 IS NULL;

-- 2.5 Registros destino que NO cruzan con el origen (quedarian sin actualizar)
SELECT 'Registros destino SIN cruce con origen' AS descripcion,
       count(*) AS total
FROM {schema}.gc_predio gp
WHERE NOT EXISTS (
    SELECT 1 FROM tmp_update_area_registral tmp
    WHERE gp.numero_predial_nacional = tmp.numero_predial
);

-- 2.6 Preview: primeros 20 registros que se actualizaran
SELECT
    gp.t_id,
    gp.numero_predial_nacional,
    gp.nupre AS nupre_destino,
    gp.area_registral_m2 AS area_actual,
    tmp.area_registral_m2 AS area_nueva,
    tmp.nupre AS nupre_origen
FROM {schema}.gc_predio gp
INNER JOIN tmp_update_area_registral tmp
    ON gp.numero_predial_nacional = tmp.numero_predial
LIMIT 20;

-- ============================================================================
-- PASO 3: Ejecutar el UPDATE
-- ============================================================================
-- Estrategia:
--   - Join principal por numero_predial_nacional = numero_predial
--   - Condicion adicional por nupre (maneja NULLs en ambos lados)
--   - Solo actualiza donde el origen tiene area_registral_m2 NOT NULL

UPDATE {schema}.gc_predio gp
SET area_registral_m2 = tmp.area_registral_m2
FROM tmp_update_area_registral tmp
WHERE gp.numero_predial_nacional = tmp.numero_predial
  AND (
      gp.nupre = tmp.nupre
      OR (gp.nupre IS NULL AND tmp.nupre IS NULL)
  )
  AND tmp.area_registral_m2 IS NOT NULL;

-- ============================================================================
-- PASO 3b (OPCIONAL): UPDATE complementario solo por numero_predial
-- ============================================================================
-- Descomentar si hay registros que no cruzaron por nupre pero si por numero_predial.
-- Util cuando el nupre difiere entre origen y destino (ej: 'BBK00000' vs valor real).
-- Solo actualiza registros que aun tienen area_registral_m2 NULL tras el PASO 3.

/*
UPDATE {schema}.gc_predio gp
SET area_registral_m2 = tmp.area_registral_m2
FROM tmp_update_area_registral tmp
WHERE gp.numero_predial_nacional = tmp.numero_predial
  AND gp.area_registral_m2 IS NULL
  AND tmp.area_registral_m2 IS NOT NULL;
*/

-- ============================================================================
-- PASO 4: Verificacion posterior
-- ============================================================================

SELECT 'Total gc_predio' AS descripcion,
       count(*) AS total
FROM {schema}.gc_predio;

SELECT 'Con area_registral_m2 NOT NULL (despues del update)' AS descripcion,
       count(*) AS total
FROM {schema}.gc_predio
WHERE area_registral_m2 IS NOT NULL;

SELECT 'Con area_registral_m2 NULL (despues del update)' AS descripcion,
       count(*) AS total
FROM {schema}.gc_predio
WHERE area_registral_m2 IS NULL;

-- Distribucion de valores actualizados (rangos)
SELECT
    CASE
        WHEN area_registral_m2 IS NULL THEN 'NULL'
        WHEN area_registral_m2 = 0 THEN '0'
        WHEN area_registral_m2 > 0 AND area_registral_m2 <= 100 THEN '1-100'
        WHEN area_registral_m2 > 100 AND area_registral_m2 <= 1000 THEN '101-1000'
        WHEN area_registral_m2 > 1000 AND area_registral_m2 <= 10000 THEN '1001-10000'
        ELSE '>10000'
    END AS rango_area_m2,
    count(*) AS total
FROM {schema}.gc_predio
GROUP BY 1
ORDER BY 1;

-- ============================================================================
-- PASO 5: Limpieza
-- ============================================================================
DROP TABLE IF EXISTS tmp_update_area_registral;
