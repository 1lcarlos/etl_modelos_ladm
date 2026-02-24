-- INSERT para tabla sinic_predio
-- Migra predios desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que existan registros en las tablas de dominios del destino
--   - Usa datos de tmp_sinic_predio (query sinic_predio.sql)

INSERT INTO {schema}.sinic_predio (
    t_id,
    --t_basket,
    t_ili_tid,
    departamento,
    municipio,
    codigo_orip,
    matricula_inmobiliaria,
    numero_predial_nacional,
    codigo_homologado,
    nupre,
    fecha_inscripcion_catastral,
    tipo_predio,
    condicion_predio,
    destinacion_economica,
    area_catastral_terreno,
    area_registral_m2,
    vigencia_actualizacion_catastral,
    estado,
    nombre,
    tipo,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    p.id::bigint,
    --nextval('{schema}.t_ili2db_seq'::regclass),
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- departamento (2 caracteres)
    COALESCE(p.departamento, '25'),

    -- municipio (3 caracteres)
    COALESCE(p.municipio, '000'),

    -- codigo_orip (4 caracteres)
    LPAD(COALESCE(p.codigo_orip, ''), 4, '0'),

    -- matricula_inmobiliaria (integer, puede ser NULL)
    CASE
        WHEN p.matricula_inmobiliaria ilike '0' THEN NULL  
        WHEN p.matricula_inmobiliaria ilike '' THEN NULL
        WHEN p.matricula_inmobiliaria IS NULL THEN NULL
        WHEN length(p.matricula_inmobiliaria) >= 6 THEN 1  
        WHEN p.matricula_inmobiliaria ~ '^[0-9]+$' THEN p.matricula_inmobiliaria::numeric
        ELSE NULL
    END as matricula_inmobiliaria,
    --p.matricula_inmobiliaria::numeric,

    -- numero_predial_nacional (NOT NULL)
    p.numero_predial_nacional,

    -- codigo_homologado (NOT NULL, 11 caracteres)
    COALESCE(p.codigo_homologado, SUBSTRING(p.numero_predial_nacional FROM 6 FOR 16)),

    -- nupre (11 caracteres)
    p.nupre,

    -- fecha_inscripcion_catastral (puede ser NULL)
    p.fecha_inscripcion_catastral::date,

    -- tipo_predio: Mapeo a cr_prediotipo
    COALESCE(
        (SELECT t_id FROM {schema}.cr_prediotipo WHERE ilicode ILIKE '%' || p.tipo_predio || '%' LIMIT 1),
        (SELECT t_id FROM {schema}.cr_prediotipo WHERE ilicode = 'Predio.Publico.Presunto_Baldio' LIMIT 1)
    ),

    -- condicion_predio: Mapeo a cr_condicionprediotipo
    COALESCE(
        (SELECT t_id FROM {schema}.cr_condicionprediotipo WHERE ilicode = p.condicion_predio LIMIT 1),
        (SELECT t_id FROM {schema}.cr_condicionprediotipo WHERE ilicode = 'NPH' LIMIT 1)
    ),

    -- destinacion_economica: Mapeo a cr_destinacioneconomicatipo
    COALESCE(
        (SELECT t_id FROM {schema}.cr_destinacioneconomicatipo WHERE ilicode = p.destinacion_economica LIMIT 1),
        CASE
            WHEN SUBSTRING(p.numero_predial_nacional FROM 6 FOR 2) = '00'
            THEN (SELECT t_id FROM {schema}.cr_destinacioneconomicatipo WHERE ilicode = 'Agricola' LIMIT 1)
            ELSE (SELECT t_id FROM {schema}.cr_destinacioneconomicatipo WHERE ilicode = 'Habitacional' LIMIT 1)
        END
    ),

    -- area_catastral_terreno (NOT NULL)
    COALESCE(p.area_catastral_terreno, '0')::numeric(25,2),

    -- area_registral_m2 (puede ser NULL)
    p.area_registral_m2::numeric(25,2),

    -- vigencia_actualizacion_catastral (NOT NULL)
    COALESCE(p.vigencia_actualizacion_catastral::date, CURRENT_DATE),

    -- estado: Mapeo a cr_estadotipo
    COALESCE(
        (SELECT t_id FROM {schema}.cr_estadotipo WHERE ilicode = p.estado LIMIT 1),
        (SELECT t_id FROM {schema}.cr_estadotipo WHERE ilicode = 'Activo' LIMIT 1)
    ),

    -- nombre
    COALESCE(NULLIF(p.nombre, ''), 'Sin_direccion'),

    -- tipo: Mapeo a col_unidadadministrativabasicatipo
    COALESCE(
        (SELECT t_id FROM {schema}.col_unidadadministrativabasicatipo
         WHERE ilicode ILIKE '%Informacion_Estadistica%' AND baseclass IS NOT NULL LIMIT 1),
        (SELECT t_id FROM {schema}.col_unidadadministrativabasicatipo LIMIT 1)
    ),

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(p.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version (puede ser NULL)
    p.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(p.espacio_de_nombres, 'SINIC_PREDIO'),

    -- local_id (NOT NULL)
    COALESCE(p.id::varchar,p.local_id)

FROM tmp_sinic_predio p;
