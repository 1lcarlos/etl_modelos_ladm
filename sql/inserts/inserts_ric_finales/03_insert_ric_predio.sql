-- INSERT para tabla ric_predio
-- Migra predios desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_gestorcatastral y ric_operadorcatastral
--   - Usa datos de tmp_predio (query predio.sql)

INSERT INTO ric.ric_predio (
    t_id,
    t_ili_tid,
    departamento,
    municipio,
    codigo_homologado,
    nupre,
    codigo_orip,
    matricula_inmobiliaria,
    numero_predial,
    numero_predial_anterior,
    fecha_inscripcion_catastral,
    condicion_predio,
    destinacion_economica,
    tipo,
    avaluo_catastral,
    zona,
    vigencia_actualizacion_catastral,
    estado,
    catastro,
    ric_gestorcatastral,
    ric_operadorcatastral,
    nombre,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    p.id::bigint,
    uuid_generate_v4(),

    -- departamento (2 caracteres)
    COALESCE(p.departamento, '25'),

    -- municipio (3 caracteres)
    COALESCE(p.municipio, '000'),

    -- codigo_homologado (usar nupre o generar uno basado en numero_predial)
    COALESCE(
        NULLIF(p.nupre, 'BBK00000'),
        SUBSTRING(p.numero_predial_nacional FROM 1 FOR 11)
    ),

    -- nupre
    p.nupre,

    -- codigo_orip
    p.codigo_orip,

    -- matricula_inmobiliaria
    p.matricula_inmobiliaria::varchar,

    -- numero_predial
    p.numero_predial_nacional,

    -- numero_predial_anterior
    p.numero_predial_anterior,

    -- fecha_inscripcion_catastral (puede ser NULL)
    NULL::date,

    -- condicion_predio: Mapeo a ric_condicionprediotipo
    COALESCE(
        (SELECT t_id FROM ric.ric_condicionprediotipo WHERE ilicode = p.condicion_predio LIMIT 1),
        (SELECT t_id FROM ric.ric_condicionprediotipo WHERE ilicode = 'NPH' LIMIT 1)
    ),

    -- destinacion_economica: Mapeo a ric_destinacioneconomicatipo
    COALESCE(
        (SELECT t_id FROM ric.ric_destinacioneconomicatipo WHERE ilicode = p.destinacion_economica LIMIT 1),
        CASE
            WHEN SUBSTRING(p.numero_predial_nacional FROM 6 FOR 2) = '00'
            THEN (SELECT t_id FROM ric.ric_destinacioneconomicatipo WHERE ilicode = 'Agricola' LIMIT 1)
            ELSE (SELECT t_id FROM ric.ric_destinacioneconomicatipo WHERE ilicode = 'Habitacional' LIMIT 1)
        END
    ),

    -- tipo: Mapeo a col_unidadadministrativabasicatipo (siempre Predio)
    COALESCE(
        (SELECT t_id FROM ric.col_unidadadministrativabasicatipo fat
         WHERE fat.ilicode ILIKE '%' || p.tipo_predio || '%' 
         AND fat.baseclass is not null
         LIMIT 1),
        (SELECT t_id FROM ric.col_unidadadministrativabasicatipo fat
         WHERE fat.ilicode = 'Predio.Privado'
         AND fat.baseclass is not null
         LIMIT 1)
    ),

    
    -- avaluo_catastral (NOT NULL, default 0)
    COALESCE(p.avaluo_catastral::numeric, 0),

    -- zona: Mapeo desde clase_suelo a ric_zonatipo (Rural/Urbano)
    COALESCE(
        (SELECT t_id FROM ric.ric_zonatipo WHERE ilicode = p.clase_suelo LIMIT 1),
        CASE
            WHEN SUBSTRING(p.numero_predial_nacional FROM 6 FOR 2) = '00'
            THEN (SELECT t_id FROM ric.ric_zonatipo WHERE ilicode = 'Rural' LIMIT 1)
            ELSE (SELECT t_id FROM ric.ric_zonatipo WHERE ilicode = 'Urbana' LIMIT 1)
        END
    ),

    -- vigencia_actualizacion_catastral (NOT NULL)
    COALESCE(p.vigencia_actualizacion_catastral::date, CURRENT_DATE),

    -- estado: Mapeo a ric_estadotipo
    COALESCE(
        (SELECT t_id FROM ric.ric_estadotipo WHERE ilicode = p.estado LIMIT 1),
        (SELECT t_id FROM ric.ric_estadotipo WHERE ilicode = 'Activo' LIMIT 1)
    ),

    -- catastro (NULL, referencia opcional)
    NULL,

    -- ric_gestorcatastral (NOT NULL)
    (SELECT t_id FROM ric.ric_gestorcatastral WHERE nombre_gestor = 'Agencia Catastral de Cundinamarca ACC' LIMIT 1),

    -- ric_operadorcatastral (NOT NULL)
    (SELECT t_id FROM ric.ric_operadorcatastral WHERE nombre_operador = 'Agencia Catastral de Cundinamarca ACC' LIMIT 1),

    -- nombre
     CASE
        WHEN p.nombre IN ('', NULL) THEN 'Sin_dirección' 
        ELSE p.nombre
    END,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(p.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version (puede ser NULL)
    p.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    'RIC_PREDIO',

    -- local_id (NOT NULL)
    COALESCE(p.id::varchar, p.numero_predial_nacional)

FROM tmp_ric_predio p;
