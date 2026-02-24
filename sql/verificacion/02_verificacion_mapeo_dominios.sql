-- ============================================================================
-- ARCHIVO 02: Verificacion de Mapeo de Dominios (Nivel 2)
-- ============================================================================
-- Ejecutar desde: pgAdmin conectado a base DESTINO (puerto 5432, db actualizacion)
-- Proposito: Verificar que los mapeos ilicode (CCA) -> text_code (Django) -> id
--            se hayan realizado correctamente para todos los campos de dominio
-- Requisito: Ejecutar 00_configuracion_dblink.sql primero
-- Fecha: 2026-02-08
-- ============================================================================

-- ============================================================================
-- TABLA 1: gc_predio (5 dominios)
-- ============================================================================

-- 1.1 gc_predio.categoria_suelo -> gc_categoriasuelotipo
SELECT
    'gc_predio' AS tabla,
    'categoria_suelo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_predio p
          JOIN cun25436.cca_categoriasuelotipo d ON p.categoria_suelo = d.t_id
          WHERE p.numero_predial IS NOT NULL
            AND p.departamento_municipio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_predio
     WHERE categoria_suelo IS NOT NULL) destino;

-- 1.2 gc_predio.clase_suelo -> gc_clasesuelotipo
SELECT
    'gc_predio' AS tabla,
    'clase_suelo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_predio p
          JOIN cun25436.cca_clasesuelotipo d ON p.clase_suelo_registro = d.t_id
          WHERE p.numero_predial IS NOT NULL
            AND p.departamento_municipio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_predio
     WHERE clase_suelo IS NOT NULL) destino;

-- 1.3 gc_predio.condicion_predio -> gc_condicionprediotipo
SELECT
    'gc_predio' AS tabla,
    'condicion_predio' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_predio p
          JOIN cun25436.cca_condicionprediotipo d ON p.condicion_predio = d.t_id
          WHERE p.numero_predial IS NOT NULL
            AND p.departamento_municipio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_predio
     WHERE condicion_predio IS NOT NULL) destino;

-- 1.4 gc_predio.destinacion_economica -> gc_destinacioneconomicatipo
SELECT
    'gc_predio' AS tabla,
    'destinacion_economica' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_predio p
          JOIN cun25436.cca_destinacioneconomicatipo d ON p.destinacion_economica = d.t_id
          WHERE p.numero_predial IS NOT NULL
            AND p.departamento_municipio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_predio
     WHERE destinacion_economica IS NOT NULL) destino;

-- 1.5 gc_predio.tipo -> gc_prediotipo
SELECT
    'gc_predio' AS tabla,
    'tipo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_predio p
          JOIN cun25436.cca_prediotipo d ON p.predio_tipo = d.t_id
          WHERE p.numero_predial IS NOT NULL
            AND p.departamento_municipio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_predio
     WHERE tipo IS NOT NULL) destino;

-- 1.6 Valores de origen sin mapeo en gc_predio (detalle)
SELECT 'gc_predio - valores_sin_mapeo' AS verificacion, *
FROM dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT 'categoria_suelo' AS campo, d.ilicode AS valor_origen, count(*) AS registros
      FROM cun25436.cca_predio p
      JOIN cun25436.cca_categoriasuelotipo d ON p.categoria_suelo = d.t_id
      WHERE p.numero_predial IS NOT NULL AND p.departamento_municipio IS NOT NULL
      GROUP BY d.ilicode
      UNION ALL
      SELECT 'clase_suelo', d.ilicode, count(*)
      FROM cun25436.cca_predio p
      JOIN cun25436.cca_clasesuelotipo d ON p.clase_suelo_registro = d.t_id
      WHERE p.numero_predial IS NOT NULL AND p.departamento_municipio IS NOT NULL
      GROUP BY d.ilicode
      UNION ALL
      SELECT 'condicion_predio', d.ilicode, count(*)
      FROM cun25436.cca_predio p
      JOIN cun25436.cca_condicionprediotipo d ON p.condicion_predio = d.t_id
      WHERE p.numero_predial IS NOT NULL AND p.departamento_municipio IS NOT NULL
      GROUP BY d.ilicode
      UNION ALL
      SELECT 'destinacion_economica', d.ilicode, count(*)
      FROM cun25436.cca_predio p
      JOIN cun25436.cca_destinacioneconomicatipo d ON p.destinacion_economica = d.t_id
      WHERE p.numero_predial IS NOT NULL AND p.departamento_municipio IS NOT NULL
      GROUP BY d.ilicode
      UNION ALL
      SELECT 'tipo_predio', SUBSTRING(d.ilicode FROM '[^.]+$$'), count(*)
      FROM cun25436.cca_predio p
      JOIN cun25436.cca_prediotipo d ON p.predio_tipo = d.t_id
      WHERE p.numero_predial IS NOT NULL AND p.departamento_municipio IS NOT NULL
      GROUP BY d.ilicode$$
) AS t(campo text, valor_origen text, registros bigint);

-- ============================================================================
-- TABLA 2: extdireccion (4 dominios)
-- ============================================================================

-- 2.1 extdireccion.tipo_direccion -> extdireccion_tipo_direccion
SELECT
    'extdireccion' AS tabla,
    'tipo_direccion' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor <= COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.extdireccion e
          INNER JOIN cun25436.cca_predio p ON p.t_id = e.cca_predio_direccion
          WHERE p.numero_predial IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.extdireccion
     WHERE tipo_direccion IS NOT NULL) destino;

-- 2.2 extdireccion.clase_via_principal -> extdireccion_clase_via_principal
SELECT
    'extdireccion' AS tabla,
    'clase_via_principal' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.extdireccion e
          INNER JOIN cun25436.cca_predio p ON p.t_id = e.cca_predio_direccion
          WHERE p.numero_predial IS NOT NULL
            AND e.clase_via_principal IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.extdireccion
     WHERE clase_via_principal IS NOT NULL) destino;

-- 2.3 extdireccion.sector_ciudad -> extdireccion_sector_ciudad
SELECT
    'extdireccion' AS tabla,
    'sector_ciudad' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.extdireccion e
          INNER JOIN cun25436.cca_predio p ON p.t_id = e.cca_predio_direccion
          WHERE p.numero_predial IS NOT NULL
            AND e.sector_ciudad IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.extdireccion
     WHERE sector_ciudad IS NOT NULL) destino;

-- 2.4 extdireccion.sector_predio -> extdireccion_sector_predio
SELECT
    'extdireccion' AS tabla,
    'sector_predio' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.extdireccion e
          INNER JOIN cun25436.cca_predio p ON p.t_id = e.cca_predio_direccion
          WHERE p.numero_predial IS NOT NULL
            AND e.sector_predio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.extdireccion
     WHERE sector_predio IS NOT NULL) destino;

-- ============================================================================
-- TABLA 3: gc_construccion (2 dominios)
-- ============================================================================

-- 3.1 gc_construccion.tipo_construccion -> gc_construcciontipo
SELECT
    'gc_construccion' AS tabla,
    'tipo_construccion' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_construccion c
          WHERE c.tipo_construccion IS NOT NULL
            AND c.predio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_construccion
     WHERE tipo_construccion IS NOT NULL) destino;

-- 3.2 gc_construccion.tipo_dominio -> gc_dominioconstrucciontipo
SELECT
    'gc_construccion' AS tabla,
    'tipo_dominio' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_construccion c
          WHERE c.tipo_dominio IS NOT NULL
            AND c.predio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_construccion
     WHERE tipo_dominio IS NOT NULL) destino;

-- ============================================================================
-- TABLA 4: gc_caracteristicasunidadconstruccion (5 dominios)
-- ============================================================================

-- 4.1 tipo_construccion
SELECT
    'gc_caracteristicasuc' AS tabla,
    'tipo_construccion' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_caracteristicasunidadconstruccion
          WHERE tipo_construccion IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_caracteristicasunidadconstruccion
     WHERE tipo_construccion IS NOT NULL) destino;

-- 4.2 tipo_dominio
SELECT
    'gc_caracteristicasuc' AS tabla,
    'tipo_dominio' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_caracteristicasunidadconstruccion
          WHERE tipo_dominio IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_caracteristicasunidadconstruccion
     WHERE tipo_dominio IS NOT NULL) destino;

-- 4.3 tipo_planta
SELECT
    'gc_caracteristicasuc' AS tabla,
    'tipo_planta' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_caracteristicasunidadconstruccion
          WHERE tipo_planta IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_caracteristicasunidadconstruccion
     WHERE tipo_planta IS NOT NULL) destino;

-- 4.4 tipo_unidad_construccion
SELECT
    'gc_caracteristicasuc' AS tabla,
    'tipo_unidad_construccion' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_caracteristicasunidadconstruccion
          WHERE tipo_unidad_construccion IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_caracteristicasunidadconstruccion
     WHERE tipo_unidad_construccion IS NOT NULL) destino;

-- 4.5 uso
SELECT
    'gc_caracteristicasuc' AS tabla,
    'uso' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_caracteristicasunidadconstruccion
          WHERE uso IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_caracteristicasunidadconstruccion
     WHERE uso IS NOT NULL) destino;

-- ============================================================================
-- TABLA 5: gc_interesado (5 dominios)
-- ============================================================================

-- 5.1 tipo
SELECT
    'gc_interesado' AS tabla,
    'tipo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_interesado
          WHERE tipo IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_interesado
     WHERE tipo IS NOT NULL) destino;

-- 5.2 tipo_documento
SELECT
    'gc_interesado' AS tabla,
    'tipo_documento' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_interesado
          WHERE tipo_documento IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_interesado
     WHERE tipo_documento IS NOT NULL) destino;

-- 5.3 sexo
SELECT
    'gc_interesado' AS tabla,
    'sexo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_interesado
          WHERE sexo IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_interesado
     WHERE sexo IS NOT NULL) destino;

-- 5.4 grupo_etnico (con mapeo especial Rrom/Negro_Afrocolombiano/Palenquero)
SELECT
    'gc_interesado' AS tabla,
    'grupo_etnico' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_interesado
          WHERE grupo_etnico IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_interesado
     WHERE grupo_etnico IS NOT NULL) destino;

-- 5.5 estado_civil
SELECT
    'gc_interesado' AS tabla,
    'estado_civil' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_interesado
          WHERE estado_civil IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_interesado
     WHERE estado_civil IS NOT NULL) destino;

-- 5.6 Detalle de valores de grupo_etnico en origen (para verificar mapeos especiales)
SELECT 'gc_interesado - grupo_etnico detalle' AS verificacion, *
FROM dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT d.ilicode AS valor_origen, count(*) AS registros
      FROM cun25436.cca_interesado i
      JOIN cun25436.cca_grupoetnicotipo d ON i.grupo_etnico = d.t_id
      GROUP BY d.ilicode
      ORDER BY registros DESC$$
) AS t(valor_origen text, registros bigint);

-- ============================================================================
-- TABLA 6: gc_agrupacioninteresados (1 dominio)
-- ============================================================================

SELECT
    'gc_agrupacioninteresados' AS tabla,
    'tipo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_agrupacioninteresados
          WHERE tipo IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_agrupacioninteresados
     WHERE tipo IS NOT NULL) destino;

-- ============================================================================
-- TABLA 7: gc_derecho (1 dominio)
-- ============================================================================

SELECT
    'gc_derecho' AS tabla,
    'tipo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_derecho
          WHERE tipo IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_derecho
     WHERE tipo IS NOT NULL) destino;

-- ============================================================================
-- TABLA 8: gc_fuenteadministrativa (1 dominio)
-- ============================================================================

SELECT
    'gc_fuenteadministrativa' AS tabla,
    'tipo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_fuenteadministrativa
          WHERE tipo IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_fuenteadministrativa
     WHERE tipo IS NOT NULL) destino;

-- ============================================================================
-- TABLA 9: gc_estructuranovedadnumeropredial (1 dominio)
-- ============================================================================

SELECT
    'gc_estructuranovedad' AS tabla,
    'tipo_novedad' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_estructuranovedadnumeropredial
          WHERE tipo_novedad IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.gc_estructuranovedadnumeropredial
     WHERE tipo_novedad IS NOT NULL) destino;

-- ============================================================================
-- TABLA 10: cuc_calificacionconvencional (1 dominio)
-- ============================================================================

SELECT
    'cuc_calificacionconv' AS tabla,
    'tipo_calificar' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_calificacionconvencional
          WHERE tipo_calificar IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.cuc_calificacionconvencional
     WHERE tipo_calificar IS NOT NULL) destino;

-- ============================================================================
-- TABLA 11: cuc_grupocalificacion (2 dominios)
-- ============================================================================

-- 11.1 clase_calificacion
SELECT
    'cuc_grupocalificacion' AS tabla,
    'clase_calificacion' AS campo_dominio,
    (SELECT count(*) FROM cun25436.cuc_grupocalificacion) AS total_registros_destino,
    (SELECT count(*) FROM cun25436.cuc_grupocalificacion WHERE clase_calificacion IS NOT NULL) AS mapeados_ok,
    (SELECT count(*) FROM cun25436.cuc_grupocalificacion WHERE clase_calificacion IS NULL) AS nulls_inesperados,
    CASE
        WHEN (SELECT count(*) FROM cun25436.cuc_grupocalificacion WHERE clase_calificacion IS NULL) = 0 THEN 'OK'
        ELSE 'FALLO'
    END AS estado;

-- 11.2 conservacion
SELECT
    'cuc_grupocalificacion' AS tabla,
    'conservacion' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT (
            (SELECT count(*) FROM cun25436.cca_calificacionconvencional WHERE conservacion_estructura IS NOT NULL) +
            (SELECT count(*) FROM cun25436.cca_calificacionconvencional WHERE conservacion_acabados IS NOT NULL) +
            (SELECT count(*) FROM cun25436.cca_calificacionconvencional WHERE conservacion_banio IS NOT NULL) +
            (SELECT count(*) FROM cun25436.cca_calificacionconvencional WHERE conservacion_cocina IS NOT NULL)
        )$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.cuc_grupocalificacion
     WHERE conservacion IS NOT NULL) destino;

-- ============================================================================
-- TABLA 12: cuc_objetoconstruccion (1 dominio)
-- ============================================================================

SELECT
    'cuc_objetoconstruccion' AS tabla,
    'tipo_objeto_construccion' AS campo_dominio,
    (SELECT count(*) FROM cun25436.cuc_objetoconstruccion) AS total_registros_destino,
    (SELECT count(*) FROM cun25436.cuc_objetoconstruccion WHERE tipo_objeto_construccion IS NOT NULL) AS mapeados_ok,
    (SELECT count(*) FROM cun25436.cuc_objetoconstruccion WHERE tipo_objeto_construccion IS NULL) AS nulls_inesperados,
    CASE
        WHEN (SELECT count(*) FROM cun25436.cuc_objetoconstruccion WHERE tipo_objeto_construccion IS NULL) = 0 THEN 'OK'
        ELSE 'FALLO'
    END AS estado;

-- ============================================================================
-- TABLA 13: cuc_calificacionnoconvencional (1 dominio)
-- ============================================================================

SELECT
    'cuc_calificacionnoconv' AS tabla,
    'tipo_anexo' AS campo_dominio,
    origen.total_con_valor AS total_con_valor_origen,
    COALESCE(destino.mapeados_ok, 0) AS mapeados_ok,
    origen.total_con_valor - COALESCE(destino.mapeados_ok, 0) AS nulls_inesperados,
    CASE
        WHEN origen.total_con_valor = COALESCE(destino.mapeados_ok, 0) THEN 'OK'
        ELSE 'FALLO'
    END AS estado
FROM
    (SELECT * FROM dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT count(*)
          FROM cun25436.cca_caracteristicasunidadconstruccion
          WHERE tipo_anexo IS NOT NULL$$
    ) AS t(total_con_valor bigint)) origen,
    (SELECT count(*) AS mapeados_ok
     FROM cun25436.cuc_calificacionnoconvencional
     WHERE tipo_anexo IS NOT NULL) destino;
