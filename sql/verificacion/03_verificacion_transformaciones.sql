-- ============================================================================
-- ARCHIVO 03: Verificacion de Transformaciones (Nivel 3)
-- ============================================================================
-- Ejecutar desde: pgAdmin conectado a base DESTINO (puerto 5432, db actualizacion)
-- Proposito: Verificar que cada transformacion critica se aplico correctamente
-- Requisito: Ejecutar 00_configuracion_dblink.sql primero
-- Fecha: 2026-02-08
-- ============================================================================

-- ============================================================================
-- 3.1 DEPARTAMENTO / MUNICIPIO (gc_predio)
-- Verificar: length(departamento)=2, length(municipio)=3
-- y que concatenados coincidan con departamento_municipio del origen
-- ============================================================================

-- 3.1.1 Verificar longitudes correctas
SELECT
    '3.1.1 - departamento length=2' AS verificacion,
    count(*) AS total_predios,
    count(*) FILTER (WHERE length(departamento) = 2) AS correctos,
    count(*) FILTER (WHERE length(departamento) != 2) AS incorrectos,
    CASE
        WHEN count(*) FILTER (WHERE length(departamento) != 2) = 0 THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM cun25436.gc_predio;

SELECT
    '3.1.2 - municipio length=3' AS verificacion,
    count(*) AS total_predios,
    count(*) FILTER (WHERE length(municipio) = 3) AS correctos,
    count(*) FILTER (WHERE length(municipio) != 3) AS incorrectos,
    CASE
        WHEN count(*) FILTER (WHERE length(municipio) != 3) = 0 THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM cun25436.gc_predio;

-- 3.1.3 Comparar concatenacion con departamento_municipio del origen via dblink
SELECT
    '3.1.3 - dept+mun vs origen' AS verificacion,
    r.total_comparados,
    r.coinciden,
    r.no_coinciden,
    CASE WHEN r.no_coinciden = 0 THEN 'OK' ELSE 'FALLO' END AS estado
FROM (
    SELECT
        count(*) AS total_comparados,
        count(*) FILTER (WHERE gp.departamento || gp.municipio = o.dept_mun) AS coinciden,
        count(*) FILTER (WHERE gp.departamento || gp.municipio != o.dept_mun) AS no_coinciden
    FROM cun25436.gc_predio gp
    INNER JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT DISTINCT ON (t_id) t_id, departamento_municipio
          FROM cun25436.cca_predio
          WHERE numero_predial IS NOT NULL AND departamento_municipio IS NOT NULL$$
    ) AS o(cca_id bigint, dept_mun text)
    ON gp.id = o.cca_id
) r;

-- ============================================================================
-- 3.2 TIENE_FMI (gc_predio) - boolean
-- Verificar: ilicode 'Si' -> true, 'No' -> false
-- ============================================================================

SELECT
    '3.2 - tiene_fmi boolean' AS verificacion,
    r.total_comparados,
    r.coinciden,
    r.no_coinciden,
    CASE WHEN r.no_coinciden = 0 THEN 'OK' ELSE 'FALLO' END AS estado
FROM (
    SELECT
        count(*) AS total_comparados,
        count(*) FILTER (WHERE
            (o.ilicode = 'Si' AND gp.tiene_fmi = true) OR
            (o.ilicode = 'No' AND gp.tiene_fmi = false) OR
            (o.ilicode IS NULL AND gp.tiene_fmi IS NULL)
        ) AS coinciden,
        count(*) FILTER (WHERE NOT (
            (o.ilicode = 'Si' AND gp.tiene_fmi = true) OR
            (o.ilicode = 'No' AND gp.tiene_fmi = false) OR
            (o.ilicode IS NULL AND gp.tiene_fmi IS NULL)
        )) AS no_coinciden
    FROM cun25436.gc_predio gp
    INNER JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT DISTINCT ON (p.t_id) p.t_id, b.ilicode
          FROM cun25436.cca_predio p
          LEFT JOIN cun25436.cca_booleanotipo b ON p.tiene_fmi = b.t_id
          WHERE p.numero_predial IS NOT NULL AND p.departamento_municipio IS NOT NULL$$
    ) AS o(cca_id bigint, ilicode text)
    ON gp.id = o.cca_id
) r;

-- ============================================================================
-- 3.3 FRACCION_DERECHO / 100 (gc_derecho)
-- Verificar: destino = origen / 100
-- ============================================================================

SELECT
    '3.3 - fraccion_derecho /100' AS verificacion,
    r.total_comparados,
    r.coinciden,
    r.no_coinciden,
    CASE WHEN r.no_coinciden = 0 THEN 'OK' ELSE 'FALLO' END AS estado
FROM (
    SELECT
        count(*) AS total_comparados,
        count(*) FILTER (WHERE
            (gd.fraccion_derecho IS NULL AND o.fraccion_origen IS NULL) OR
            (gd.fraccion_derecho = (o.fraccion_origen::numeric / 100.0)::numeric)
        ) AS coinciden,
        count(*) FILTER (WHERE NOT (
            (gd.fraccion_derecho IS NULL AND o.fraccion_origen IS NULL) OR
            (gd.fraccion_derecho = (o.fraccion_origen::numeric / 100.0)::numeric)
        )) AS no_coinciden
    FROM cun25436.gc_derecho gd
    INNER JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT t_id, fraccion_derecho
          FROM cun25436.cca_derecho$$
    ) AS o(cca_id bigint, fraccion_origen text)
    ON gd.id = o.cca_id
) r;

-- Detalle de discrepancias (si las hay)
SELECT
    '3.3 detalle' AS verificacion,
    gd.id,
    gd.fraccion_derecho AS valor_destino,
    o.fraccion_origen AS valor_origen,
    (o.fraccion_origen::numeric / 100.0)::numeric AS esperado
FROM cun25436.gc_derecho gd
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT t_id, fraccion_derecho
      FROM cun25436.cca_derecho
      WHERE fraccion_derecho IS NOT NULL$$
) AS o(cca_id bigint, fraccion_origen numeric)
ON gd.id = o.cca_id
WHERE gd.fraccion_derecho != (o.fraccion_origen / 100.0)::numeric
   OR (gd.fraccion_derecho IS NULL AND o.fraccion_origen IS NOT NULL)
LIMIT 20;

-- ============================================================================
-- 3.4 GEOMETRIAS (gc_terreno, gc_construccion, gc_unidadconstruccion)
-- Verificar: ST_GeometryType, ST_SRID=9377, ST_NDims=3
-- ============================================================================

-- 3.4.1 gc_terreno
SELECT
    '3.4.1 - geometria gc_terreno' AS verificacion,
    count(*) AS total,
    count(*) FILTER (WHERE geometria IS NOT NULL) AS con_geometria,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_SRID(geometria) = 9377) AS srid_ok,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_NDims(geometria) = 3) AS ndims_ok,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_GeometryType(geometria) IN ('ST_MultiPolygon', 'ST_MultiPolygonZ')) AS tipo_ok,
    CASE
        WHEN count(*) FILTER (WHERE geometria IS NOT NULL AND ST_SRID(geometria) != 9377) = 0
             AND count(*) FILTER (WHERE geometria IS NOT NULL AND ST_NDims(geometria) != 3) = 0
        THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM cun25436.gc_terreno;

-- 3.4.2 gc_construccion
SELECT
    '3.4.2 - geometria gc_construccion' AS verificacion,
    count(*) AS total,
    count(*) FILTER (WHERE geometria IS NOT NULL) AS con_geometria,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_SRID(geometria) = 9377) AS srid_ok,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_NDims(geometria) = 3) AS ndims_ok,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_GeometryType(geometria) IN ('ST_MultiPolygon', 'ST_MultiPolygonZ')) AS tipo_ok,
    CASE
        WHEN count(*) FILTER (WHERE geometria IS NOT NULL AND ST_SRID(geometria) != 9377) = 0
             AND count(*) FILTER (WHERE geometria IS NOT NULL AND ST_NDims(geometria) != 3) = 0
        THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM cun25436.gc_construccion;

-- 3.4.3 gc_unidadconstruccion
SELECT
    '3.4.3 - geometria gc_unidadconstruccion' AS verificacion,
    count(*) AS total,
    count(*) FILTER (WHERE geometria IS NOT NULL) AS con_geometria,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_SRID(geometria) = 9377) AS srid_ok,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_NDims(geometria) = 3) AS ndims_ok,
    count(*) FILTER (WHERE geometria IS NOT NULL AND ST_GeometryType(geometria) IN ('ST_MultiPolygon', 'ST_MultiPolygonZ')) AS tipo_ok,
    CASE
        WHEN count(*) FILTER (WHERE geometria IS NOT NULL AND ST_SRID(geometria) != 9377) = 0
             AND count(*) FILTER (WHERE geometria IS NOT NULL AND ST_NDims(geometria) != 3) = 0
        THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM cun25436.gc_unidadconstruccion;

-- ============================================================================
-- 3.5 SPLIT NOMBRE RECONOCEDOR (dlc_datosadicionaleslevantamientocatastral)
-- Verificar: campos no esten vacios cuando origen tenia nombre
-- ============================================================================

SELECT
    '3.5 - nombre reconocedor split' AS verificacion,
    r.total_con_nombre_origen,
    r.primer_nombre_no_vacio,
    r.primer_apellido_no_vacio,
    CASE
        WHEN r.total_con_nombre_origen = 0 THEN 'N/A'
        WHEN r.primer_nombre_no_vacio > 0 OR r.primer_apellido_no_vacio > 0 THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM (
    SELECT
        count(*) FILTER (WHERE o.nombre_usuario IS NOT NULL AND o.nombre_usuario != '') AS total_con_nombre_origen,
        count(*) FILTER (WHERE dlc.primer_nombre_reconocedor IS NOT NULL AND dlc.primer_nombre_reconocedor != '') AS primer_nombre_no_vacio,
        count(*) FILTER (WHERE dlc.primer_apellido_reconocedor IS NOT NULL AND dlc.primer_apellido_reconocedor != '') AS primer_apellido_no_vacio
    FROM cun25436.dlc_datosadicionaleslevantamientocatastral dlc
    INNER JOIN cun25436.gc_predio gp ON dlc.gc_predio = gp.id
    LEFT JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT DISTINCT ON (p.t_id) p.t_id, u.nombre
          FROM cun25436.cca_predio p
          LEFT JOIN cun25436.cca_usuario u ON p.usuario = u.t_id
          WHERE p.numero_predial IS NOT NULL$$
    ) AS o(cca_id bigint, nombre_usuario text)
    ON gp.id = o.cca_id
) r;

-- ============================================================================
-- 3.6 OBSERVACION -> DESCRIPCION (gc_derecho, gc_fuenteadministrativa)
-- Verificar: campo descripcion contiene el valor de observacion del origen
-- ============================================================================

-- 3.6.1 gc_derecho: observacion -> descripcion
SELECT
    '3.6.1 - gc_derecho obs->desc' AS verificacion,
    r.total_comparados,
    r.coinciden,
    r.no_coinciden,
    CASE WHEN r.no_coinciden = 0 THEN 'OK' ELSE 'FALLO' END AS estado
FROM (
    SELECT
        count(*) AS total_comparados,
        count(*) FILTER (WHERE
            (gd.descripcion IS NULL AND o.observacion IS NULL) OR
            (gd.descripcion = o.observacion)
        ) AS coinciden,
        count(*) FILTER (WHERE NOT (
            (gd.descripcion IS NULL AND o.observacion IS NULL) OR
            (gd.descripcion = o.observacion)
        )) AS no_coinciden
    FROM cun25436.gc_derecho gd
    INNER JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT t_id, observacion FROM cun25436.cca_derecho$$
    ) AS o(cca_id bigint, observacion text)
    ON gd.id = o.cca_id
) r;

-- 3.6.2 gc_fuenteadministrativa: observacion -> descripcion
SELECT
    '3.6.2 - gc_fuenteadm obs->desc' AS verificacion,
    r.total_comparados,
    r.coinciden,
    r.no_coinciden,
    CASE WHEN r.no_coinciden = 0 THEN 'OK' ELSE 'FALLO' END AS estado
FROM (
    SELECT
        count(*) AS total_comparados,
        count(*) FILTER (WHERE
            (gf.descripcion IS NULL AND o.observacion IS NULL) OR
            (gf.descripcion = o.observacion)
        ) AS coinciden,
        count(*) FILTER (WHERE NOT (
            (gf.descripcion IS NULL AND o.observacion IS NULL) OR
            (gf.descripcion = o.observacion)
        )) AS no_coinciden
    FROM cun25436.gc_fuenteadministrativa gf
    INNER JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT t_id, observacion FROM cun25436.cca_fuenteadministrativa$$
    ) AS o(cca_id bigint, observacion text)
    ON gf.id = o.cca_id
) r;

-- ============================================================================
-- 3.7 VALORES POR DEFECTO (gc_predio)
-- Verificar: interrelacionado=false, nupre_fmi=false, rectificacion_efecto_registral=false
-- ============================================================================

SELECT
    '3.7 - valores por defecto gc_predio' AS verificacion,
    count(*) AS total_predios,
    count(*) FILTER (WHERE interrelacionado = false) AS interrelacionado_false,
    count(*) FILTER (WHERE nupre_fmi = false) AS nupre_fmi_false,
    count(*) FILTER (WHERE rectificacion_efecto_registral = false) AS rectificacion_false,
    CASE
        WHEN count(*) FILTER (WHERE interrelacionado != false OR nupre_fmi != false OR rectificacion_efecto_registral != false) = 0
        THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM cun25436.gc_predio;

-- ============================================================================
-- 3.8 LOCALIZACION POR DEFECTO (extdireccion)
-- Verificar: cuando origen es NULL, destino tiene punto fijo
-- Punto fijo: ST_Point(4940023.7497, 2111479.6705, 9377)
-- ============================================================================

SELECT
    '3.8 - localizacion por defecto' AS verificacion,
    r.total_comparados,
    r.origen_null_destino_tiene_punto,
    r.origen_null_destino_null,
    CASE
        WHEN r.origen_null_destino_null = 0 THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM (
    SELECT
        count(*) AS total_comparados,
        count(*) FILTER (WHERE o.loc_es_null AND ed.localizacion IS NOT NULL) AS origen_null_destino_tiene_punto,
        count(*) FILTER (WHERE o.loc_es_null AND ed.localizacion IS NULL) AS origen_null_destino_null
    FROM cun25436.extdireccion ed
    INNER JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT e.t_id, (e.localizacion IS NULL) AS loc_es_null
          FROM cun25436.extdireccion e
          INNER JOIN cun25436.cca_predio p ON p.t_id = e.cca_predio_direccion
          WHERE p.numero_predial IS NOT NULL$$
    ) AS o(cca_id bigint, loc_es_null boolean)
    ON ed.id = o.cca_id
) r;

-- Verificar el valor del punto fijo asignado
SELECT
    '3.8.1 - punto fijo valor' AS verificacion,
    count(*) AS total_con_punto_defecto,
    count(*) FILTER (WHERE
        abs(ST_X(localizacion) - 4940023.7497) < 0.001
        AND abs(ST_Y(localizacion) - 2111479.6705) < 0.001
    ) AS punto_correcto,
    CASE
        WHEN count(*) > 0 THEN 'INFO'
        ELSE 'N/A'
    END AS estado
FROM cun25436.extdireccion
WHERE localizacion IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dblink(
          'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
          $$SELECT e.t_id
            FROM cun25436.extdireccion e
            WHERE e.localizacion IS NOT NULL$$
      ) AS o(cca_id bigint)
      WHERE o.cca_id = cun25436.extdireccion.id
  );
