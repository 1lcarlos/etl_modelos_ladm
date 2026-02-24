-- INSERT para tabla ric_interesado
-- Migra interesados desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Usa datos de tmp_interesado (query interesado.sql)

INSERT INTO ric.ric_interesado (
    t_id,
    t_ili_tid,
    tipo,
    tipo_documento,
    documento_identidad,
    primer_nombre,
    segundo_nombre,
    primer_apellido,
    segundo_apellido,
    sexo,
    grupoetnico,
    razon_social,
    estado_civil,
    nombre,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    i.id_interesado::bigint,
    uuid_generate_v4(),

    -- tipo: Mapeo a ric_interesadotipo (Persona_Natural o Persona_Juridica)
    COALESCE(
        (SELECT t_id FROM ric.ric_interesadotipo
         WHERE ilicode ILIKE '%' || i.interesadotipo || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.ric_interesadotipo
         WHERE ilicode = 'Persona_Natural'
         LIMIT 1)
    ),

    -- tipo_documento: Mapeo a ric_interesadodocumentotipo
    COALESCE(
        (SELECT t_id FROM ric.ric_interesadodocumentotipo
         WHERE ilicode ILIKE '%' || i.tipo_documento || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.ric_interesadodocumentotipo
         WHERE ilicode = 'Cedula_Ciudadania'
         LIMIT 1)
    ),

    -- documento_identidad (NOT NULL)
    COALESCE(i.documento_identidad, 'SIN_DOCUMENTO'),

    -- primer_nombre
    i.primer_nombre,

    -- segundo_nombre
    i.segundo_nombre,

    -- primer_apellido
    i.primer_apellido,

    -- segundo_apellido
    i.segundo_apellido,

    -- sexo: Mapeo a ric_sexotipo
    (SELECT t_id FROM ric.ric_sexotipo
     WHERE ilicode ILIKE '%' || i.sexo || '%'
     LIMIT 1),

    -- grupoetnico: Mapeo a ric_grupoetnicotipo
    (SELECT t_id FROM ric.ric_grupoetnicotipo
     WHERE ilicode ILIKE '%' || i.grupo_etnico || '%'
     LIMIT 1),

    -- razon_social (para personas juridicas)
    i.razon_social,

    -- estado_civil: Mapeo a ric_estadociviltipo
    (SELECT t_id FROM ric.ric_estadociviltipo
     WHERE ilicode ILIKE '%' || i.estado_civil || '%'
     LIMIT 1),

    -- nombre
    i.nombre,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(i.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version
    NULL::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(i.espacio_de_nombres, 'RIC_INTERESADO'),

    -- local_id (NOT NULL)
    COALESCE(i.id_interesado::varchar, i.local_id)

FROM tmp_interesado i
WHERE i.documento_identidad IS NOT NULL;
