-- ============================================================================
-- ARCHIVO 05: Verificacion de Muestreo Campo a Campo (Nivel 5)
-- ============================================================================
-- Ejecutar desde: pgAdmin conectado a base DESTINO (puerto 5432, db actualizacion)
-- Proposito: Comparacion directa de valores para muestra (LIMIT 20) via dblink
-- Requisito: Ejecutar 00_configuracion_dblink.sql primero
-- Fecha: 2026-02-08
-- ============================================================================

-- ============================================================================
-- 5.1 gc_predio vs cca_predio
-- Campos: numero_predial, nupre, departamento, municipio, tiene_fmi,
--         codigo_orip, matricula_inmobiliaria, nombre
-- ============================================================================

SELECT
    gp.id,
    campo,
    valor_origen,
    valor_destino,
    CASE WHEN valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL) THEN 'SI' ELSE 'NO' END AS coincide
FROM cun25436.gc_predio gp
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT DISTINCT ON (p.t_id)
        p.t_id,
        p.numero_predial,
        p.nupre,
        SUBSTRING(p.departamento_municipio, 1, 2) AS departamento,
        SUBSTRING(p.departamento_municipio, 3, 3) AS municipio,
        b.ilicode AS tiene_fmi,
        p.codigo_orip,
        p.matricula_inmobiliaria,
        e.nombre_predio
      FROM cun25436.cca_predio p
      LEFT JOIN cun25436.cca_booleanotipo b ON p.tiene_fmi = b.t_id
      LEFT JOIN cun25436.extdireccion e ON p.t_id = e.cca_predio_direccion
      WHERE p.numero_predial IS NOT NULL AND p.departamento_municipio IS NOT NULL
      ORDER BY p.t_id$$
) AS o(cca_id bigint, numero_predial text, nupre text, departamento text,
       municipio text, tiene_fmi text, codigo_orip text,
       matricula_inmobiliaria text, nombre_predio text)
ON gp.id = o.cca_id
CROSS JOIN LATERAL (VALUES
    ('numero_predial', o.numero_predial, gp.numero_predial),
    ('nupre', o.nupre, gp.nupre),
    ('departamento', o.departamento, gp.departamento),
    ('municipio', o.municipio, gp.municipio),
    ('tiene_fmi',
        CASE WHEN o.tiene_fmi = 'Si' THEN 'true' WHEN o.tiene_fmi = 'No' THEN 'false' ELSE NULL END,
        gp.tiene_fmi::text),
    ('codigo_orip', o.codigo_orip, gp.codigo_orip),
    ('matricula_inmobiliaria', o.matricula_inmobiliaria, gp.matricula_inmobiliaria::text),
    ('nombre', o.nombre_predio, gp.nombre)
) AS t(campo, valor_origen, valor_destino)
ORDER BY gp.id
LIMIT 160; -- 20 registros x 8 campos

-- ============================================================================
-- 5.2 gc_terreno vs cca_terreno
-- Campos: area, etiqueta, tiene_geometria
-- ============================================================================

SELECT
    gt.id,
    campo,
    valor_origen,
    valor_destino,
    CASE WHEN valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL) THEN 'SI' ELSE 'NO' END AS coincide
FROM cun25436.gc_terreno gt
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT
        t.t_id,
        t.area_terreno::text,
        t.etiqueta,
        CASE WHEN t.geometria IS NOT NULL THEN 'true' ELSE 'false' END AS tiene_geometria
      FROM cun25436.cca_terreno t
      WHERE t.geometria IS NOT NULL$$
) AS o(cca_id bigint, area_terreno text, etiqueta text, tiene_geometria text)
ON gt.id = o.cca_id
CROSS JOIN LATERAL (VALUES
    ('area', o.area_terreno, gt.area::text),
    ('etiqueta', o.etiqueta, gt.etiqueta),
    ('tiene_geometria', o.tiene_geometria,
        CASE WHEN gt.geometria IS NOT NULL THEN 'true' ELSE 'false' END)
) AS t(campo, valor_origen, valor_destino)
ORDER BY gt.id
LIMIT 60; -- 20 registros x 3 campos

-- ============================================================================
-- 5.3 gc_interesado vs cca_interesado
-- Campos: documento_identidad, primer_nombre, primer_apellido,
--         razon_social, nombre
-- ============================================================================

SELECT
    gi.id,
    campo,
    valor_origen,
    valor_destino,
    CASE WHEN valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL) THEN 'SI' ELSE 'NO' END AS coincide
FROM cun25436.gc_interesado gi
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT
        i.t_id,
        i.documento_identidad,
        i.primer_nombre,
        i.primer_apellido,
        i.razon_social,
        i.nombre
      FROM cun25436.cca_interesado i$$
) AS o(cca_id bigint, documento_identidad text, primer_nombre text,
       primer_apellido text, razon_social text, nombre text)
ON gi.id = o.cca_id
CROSS JOIN LATERAL (VALUES
    ('documento_identidad', o.documento_identidad, gi.documento_identidad),
    ('primer_nombre', o.primer_nombre, gi.primer_nombre),
    ('primer_apellido', o.primer_apellido, gi.primer_apellido),
    ('razon_social', o.razon_social, gi.razon_social),
    ('nombre', o.nombre, gi.nombre)
) AS t(campo, valor_origen, valor_destino)
ORDER BY gi.id
LIMIT 100; -- 20 registros x 5 campos

-- ============================================================================
-- 5.4 gc_derecho vs cca_derecho
-- Campos: fraccion_derecho (con /100), fecha_inicio_tenencia,
--         descripcion vs observacion
-- ============================================================================

SELECT
    gd.id,
    campo,
    valor_origen,
    valor_destino,
    CASE WHEN valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL) THEN 'SI' ELSE 'NO' END AS coincide
FROM cun25436.gc_derecho gd
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT
        d.t_id,
        CASE
            WHEN d.fraccion_derecho IS NOT NULL
            THEN (d.fraccion_derecho::numeric / 100.0)::text
            ELSE NULL
        END AS fraccion_derecho_convertido,
        d.fecha_inicio_tenencia::text,
        d.observacion
      FROM cun25436.cca_derecho d$$
) AS o(cca_id bigint, fraccion_derecho text, fecha_inicio_tenencia text, observacion text)
ON gd.id = o.cca_id
CROSS JOIN LATERAL (VALUES
    ('fraccion_derecho', o.fraccion_derecho, gd.fraccion_derecho::text),
    ('fecha_inicio_tenencia', o.fecha_inicio_tenencia, gd.fecha_inicio_tenencia::text),
    ('descripcion_vs_observacion', o.observacion, gd.descripcion)
) AS t(campo, valor_origen, valor_destino)
ORDER BY gd.id
LIMIT 60; -- 20 registros x 3 campos

-- ============================================================================
-- 5.5 gc_fuenteadministrativa vs cca_fuenteadministrativa
-- Campos: numero_fuente, fecha_documento_fuente, ente_emisor,
--         descripcion vs observacion
-- ============================================================================

SELECT
    gf.id,
    campo,
    valor_origen,
    valor_destino,
    CASE WHEN valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL) THEN 'SI' ELSE 'NO' END AS coincide
FROM cun25436.gc_fuenteadministrativa gf
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT
        f.t_id,
        f.numero_fuente,
        f.fecha_documento_fuente::text,
        f.ente_emisor,
        f.observacion
      FROM cun25436.cca_fuenteadministrativa f$$
) AS o(cca_id bigint, numero_fuente text, fecha_documento_fuente text,
       ente_emisor text, observacion text)
ON gf.id = o.cca_id
CROSS JOIN LATERAL (VALUES
    ('numero_fuente', o.numero_fuente, gf.numero_fuente),
    ('fecha_documento_fuente', o.fecha_documento_fuente, gf.fecha_documento_fuente::text),
    ('ente_emisor', o.ente_emisor, gf.ente_emisor),
    ('descripcion_vs_observacion', o.observacion, gf.descripcion)
) AS t(campo, valor_origen, valor_destino)
ORDER BY gf.id
LIMIT 80; -- 20 registros x 4 campos

-- ============================================================================
-- 5.6 gc_construccion vs cca_construccion
-- Campos: numero_pisos, area, anio_construccion, altura
-- ============================================================================

SELECT
    gc.id,
    campo,
    valor_origen,
    valor_destino,
    CASE WHEN valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL) THEN 'SI' ELSE 'NO' END AS coincide
FROM cun25436.gc_construccion gc
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT
        c.t_id,
        c.numero_pisos::text,
        c.area_construccion_digital::text,
        c.anio_construccion::text,
        c.altura::text
      FROM cun25436.cca_construccion c$$
) AS o(cca_id bigint, numero_pisos text, area text, anio_construccion text, altura text)
ON gc.id = o.cca_id
CROSS JOIN LATERAL (VALUES
    ('numero_pisos', o.numero_pisos, gc.numero_pisos::text),
    ('area', o.area, gc.area::text),
    ('anio_construccion', o.anio_construccion, gc.anio_construccion::text),
    ('altura', o.altura, gc.altura::text)
) AS t(campo, valor_origen, valor_destino)
ORDER BY gc.id
LIMIT 80; -- 20 registros x 4 campos

-- ============================================================================
-- 5.7 dlc_datosadicionaleslevantamientocatastral vs cca_predio+usuario
-- Campos: observaciones, fecha_visita_predial, numero_documento_reconocedor
-- ============================================================================

SELECT
    dlc.id,
    campo,
    valor_origen,
    valor_destino,
    CASE WHEN valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL) THEN 'SI' ELSE 'NO' END AS coincide
FROM cun25436.dlc_datosadicionaleslevantamientocatastral dlc
INNER JOIN cun25436.gc_predio gp ON dlc.gc_predio = gp.id
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT DISTINCT ON (p.t_id)
        p.t_id,
        p.observaciones,
        p.fecha_visita_predial::text,
        u.numero_documento
      FROM cun25436.cca_predio p
      LEFT JOIN cun25436.cca_usuario u ON p.usuario = u.t_id
      WHERE p.numero_predial IS NOT NULL$$
) AS o(cca_id bigint, observaciones text, fecha_visita_predial text,
       numero_documento text)
ON gp.id = o.cca_id
CROSS JOIN LATERAL (VALUES
    ('observaciones', o.observaciones, dlc.observaciones),
    ('fecha_visita_predial', o.fecha_visita_predial, dlc.fecha_visita_predial::text),
    ('numero_documento_reconocedor', o.numero_documento, dlc.numero_documento_reconocedor)
) AS t(campo, valor_origen, valor_destino)
ORDER BY dlc.id
LIMIT 60; -- 20 registros x 3 campos

-- ============================================================================
-- RESUMEN: Conteo de discrepancias por tabla
-- ============================================================================

SELECT 'RESUMEN MUESTREO' AS seccion;

-- Resumen gc_predio
SELECT
    'gc_predio' AS tabla,
    count(*) AS campos_comparados,
    count(*) FILTER (WHERE valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL)) AS coinciden,
    count(*) FILTER (WHERE NOT (valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL))) AS no_coinciden
FROM (
    SELECT o.numero_predial AS valor_origen, gp.numero_predial AS valor_destino
    FROM cun25436.gc_predio gp
    INNER JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT DISTINCT ON (p.t_id) p.t_id, p.numero_predial
          FROM cun25436.cca_predio p
          WHERE p.numero_predial IS NOT NULL AND p.departamento_municipio IS NOT NULL$$
    ) AS o(cca_id bigint, numero_predial text)
    ON gp.id = o.cca_id
) sub;

-- Resumen gc_interesado
SELECT
    'gc_interesado' AS tabla,
    count(*) AS campos_comparados,
    count(*) FILTER (WHERE valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL)) AS coinciden,
    count(*) FILTER (WHERE NOT (valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL))) AS no_coinciden
FROM (
    SELECT o.documento_identidad AS valor_origen, gi.documento_identidad AS valor_destino
    FROM cun25436.gc_interesado gi
    INNER JOIN dblink(
        'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
        $$SELECT t_id, documento_identidad FROM cun25436.cca_interesado$$
    ) AS o(cca_id bigint, documento_identidad text)
    ON gi.id = o.cca_id
) sub;

-- Resumen gc_derecho (fraccion_derecho convertido)
SELECT
    'gc_derecho (fraccion)' AS tabla,
    count(*) AS campos_comparados,
    count(*) FILTER (WHERE
        (gd.fraccion_derecho IS NULL AND o.fraccion_origen IS NULL) OR
        (gd.fraccion_derecho = (o.fraccion_origen / 100.0)::numeric)
    ) AS coinciden,
    count(*) FILTER (WHERE NOT (
        (gd.fraccion_derecho IS NULL AND o.fraccion_origen IS NULL) OR
        (gd.fraccion_derecho = (o.fraccion_origen / 100.0)::numeric)
    )) AS no_coinciden
FROM cun25436.gc_derecho gd
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT t_id, fraccion_derecho FROM cun25436.cca_derecho$$
) AS o(cca_id bigint, fraccion_origen numeric)
ON gd.id = o.cca_id;




SELECT
    gi.id,
    campo,
    valor_origen,
    valor_destino,
    CASE WHEN valor_origen = valor_destino OR (valor_origen IS NULL AND valor_destino IS NULL) THEN 'SI' ELSE 'NO' END AS coincide
FROM (
    SELECT * FROM cun25436.gc_interesado
    ORDER BY RANDOM()
    LIMIT 100
) gi
INNER JOIN dblink(
    'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123',
    $$SELECT
        i.t_id,
        i.documento_identidad,
        i.primer_nombre,
        i.primer_apellido,
        i.razon_social,
        i.nombre
      FROM cun25436.cca_interesado i$$
) AS o(cca_id bigint, documento_identidad text, primer_nombre text,
       primer_apellido text, razon_social text, nombre text)
ON gi.id = o.cca_id
CROSS JOIN LATERAL (VALUES
    ('documento_identidad', o.documento_identidad, gi.documento_identidad),
    ('primer_nombre', o.primer_nombre, gi.primer_nombre),
    ('primer_apellido', o.primer_apellido, gi.primer_apellido),
    ('razon_social', o.razon_social, gi.razon_social),
    ('nombre', o.nombre, gi.nombre)
) AS t(campo, valor_origen, valor_destino)
ORDER BY gi.id;