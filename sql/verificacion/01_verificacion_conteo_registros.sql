-- ============================================================================
-- ARCHIVO 01: Verificacion de Conteo de Registros (Nivel 1)
-- ============================================================================
-- Ejecutar desde: pgAdmin conectado a base DESTINO (puerto 5432, db actualizacion)
-- Proposito: Comparar conteos origen (CCA) vs destino (Django) para las 22 migraciones
-- Requisito: Ejecutar 00_configuracion_dblink.sql primero
-- Fecha: 2026-02-08
-- ============================================================================

-- Constante de conexion dblink
-- 'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123'

WITH conteos AS (

    -- 01. gc_predio
    SELECT
        '01_gc_predio' AS tabla_destino,
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(DISTINCT (p.t_id, p.numero_predial))
              FROM cun25436.cca_predio p
              LEFT JOIN cun25436.extdireccion e ON p.t_id = e.cca_predio_direccion
              LEFT JOIN cun25436.cca_terreno t ON p.t_id = t.predio
              LEFT JOIN cun25436.cca_construccion c ON p.t_id = c.predio
              WHERE p.numero_predial IS NOT NULL
                AND SUBSTRING(p.departamento_municipio, 1, 2) IS NOT NULL
                AND SUBSTRING(p.departamento_municipio, 3, 3) IS NOT NULL$$
        ) AS t(cnt bigint)) AS count_origen,
        (SELECT count(*) FROM cun25436.gc_predio) AS count_destino

    UNION ALL

    -- 02. gc_terreno
    SELECT
        '02_gc_terreno',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_terreno t
              WHERE t.geometria IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_terreno)

    UNION ALL

    -- 03. extdireccion
    SELECT
        '03_extdireccion',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.extdireccion e
              INNER JOIN cun25436.cca_predio p ON p.t_id = e.cca_predio_direccion
              WHERE p.numero_predial IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.extdireccion)

    UNION ALL

    -- 04. col_uebaunit (terreno)
    SELECT
        '04_col_uebaunit_terreno',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_terreno t
              WHERE t.geometria IS NOT NULL
                AND t.predio IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_terreno IS NOT NULL)

    UNION ALL

    -- 05. dlc_datosadicionaleslevantamientocatastral
    SELECT
        '05_dlc_datosadicionales',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_predio p
              WHERE p.numero_predial IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.dlc_datosadicionaleslevantamientocatastral)

    UNION ALL

    -- 06. gc_estructuranovedadnumeropredial
    SELECT
        '06_gc_estructuranovedad',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_estructuranovedadnumeropredial e
              INNER JOIN cun25436.cca_predio p ON p.t_id = e.cca_predio_novedad_numeros_prediales
              WHERE p.numero_predial IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_estructuranovedadnumeropredial)

    UNION ALL

    -- 07. gc_construccion
    SELECT
        '07_gc_construccion',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_construccion c
              WHERE c.predio IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_construccion)

    UNION ALL

    -- 08. gc_caracteristicasunidadconstruccion
    SELECT
        '08_gc_caracteristicasuc',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_caracteristicasunidadconstruccion$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_caracteristicasunidadconstruccion)

    UNION ALL

    -- 09. gc_unidadconstruccion
    SELECT
        '09_gc_unidadconstruccion',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_unidadconstruccion uc
              WHERE uc.construccion IS NOT NULL
                AND uc.caracteristicasunidadconstruccion IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_unidadconstruccion)

    UNION ALL

    -- 10. col_uebaunit (construccion)
    SELECT
        '10_col_uebaunit_construccion',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_construccion c
              WHERE c.predio IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_construccion IS NOT NULL)

    UNION ALL

    -- 11. col_uebaunit (unidadconstruccion)
    SELECT
        '11_col_uebaunit_unidadconst',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_unidadconstruccion uc
              WHERE uc.construccion IS NOT NULL
                AND uc.caracteristicasunidadconstruccion IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.col_uebaunit WHERE ue_gc_unidadconstruccion IS NOT NULL)

    UNION ALL

    -- 12. cuc_calificacionconvencional
    SELECT
        '12_cuc_calificacionconv',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_calificacionconvencional$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.cuc_calificacionconvencional)

    UNION ALL

    -- 13. cuc_grupocalificacion (1 fila origen -> 5 filas destino)
    SELECT
        '13_cuc_grupocalificacion',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*) * 5
              FROM cun25436.cca_calificacionconvencional$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.cuc_grupocalificacion)

    UNION ALL

    -- 14. cuc_objetoconstruccion (hasta 13 componentes no NULL por fila origen)
    SELECT
        '14_cuc_objetoconstruccion',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT sum(
                CASE WHEN armazon IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN muros IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN cubierta IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN fachada IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN cubrimiento_muros IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN piso IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN tamanio_banio IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN enchape_banio IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN mobiliario_banio IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN tamanio_cocina IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN enchape_cocina IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN mobiliario_cocina IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN cerchas IS NOT NULL THEN 1 ELSE 0 END
              )
              FROM cun25436.cca_calificacionconvencional$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.cuc_objetoconstruccion)

    UNION ALL

    -- 15. cuc_calificacionnoconvencional (solo tipo_anexo IS NOT NULL)
    SELECT
        '15_cuc_calificacionnoconv',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_caracteristicasunidadconstruccion
              WHERE tipo_anexo IS NOT NULL$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.cuc_calificacionnoconvencional)

    UNION ALL

    -- 16. gc_interesado
    SELECT
        '16_gc_interesado',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_interesado$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_interesado)

    UNION ALL

    -- 17. gc_agrupacioninteresados
    SELECT
        '17_gc_agrupacioninteresados',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_agrupacioninteresados$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_agrupacioninteresados)

    UNION ALL

    -- 18. col_miembros
    SELECT
        '18_col_miembros',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_miembros$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.col_miembros)

    UNION ALL

    -- 19. gc_derecho
    SELECT
        '19_gc_derecho',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_derecho$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_derecho)

    UNION ALL

    -- 20. gc_fuenteadministrativa
    SELECT
        '20_gc_fuenteadministrativa',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_fuenteadministrativa$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.gc_fuenteadministrativa)

    UNION ALL

    -- 21. col_rrrfuente
    SELECT
        '21_col_rrrfuente',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_fuenteadministrativa_derecho$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.col_rrrfuente)

    UNION ALL

    -- 22. col_unidadfuente
    SELECT
        '22_col_unidadfuente',
        (SELECT * FROM dblink(
            'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
            $$SELECT count(*)
              FROM cun25436.cca_unidadfuente$$
        ) AS t(cnt bigint)),
        (SELECT count(*) FROM cun25436.col_unidadfuente)
)
SELECT
    tabla_destino,
    count_origen,
    count_destino,
    count_destino - count_origen AS diferencia,
    CASE
        WHEN count_destino = count_origen THEN 'OK'
        WHEN count_destino > count_origen THEN 'EXCESO'
        ELSE 'FALLO'
    END AS estado
FROM conteos
ORDER BY tabla_destino;
