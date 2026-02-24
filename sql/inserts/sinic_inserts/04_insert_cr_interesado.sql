-- INSERT para tabla cr_interesado
-- Migra interesados desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Usa datos de tmp_sinic_interesado (query cr_interesado.sql)

INSERT INTO {schema}.cr_interesado (
    t_id,
    --t_basket,
    t_ili_tid,
    tipo_documento,
    primer_nombre,
    segundo_nombre,
    primer_apellido,
    segundo_apellido,
    sexo,
    autoreconocimientoetnico,
    autoreconocimientocampesino,
    razon_social,
    nombre,
    tipo_interesado,
    numero_documento,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    i.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- tipo_documento: Mapeo a col_documentotipo
    COALESCE(
        (SELECT t_id FROM {schema}.col_documentotipo WHERE ilicode = i.tipo_documento LIMIT 1),
        (SELECT t_id FROM {schema}.col_documentotipo WHERE ilicode = 'Cedula_Ciudadania' LIMIT 1)
    ),

    -- primer_nombre
    i.primer_nombre,

    -- segundo_nombre
    i.segundo_nombre,

    -- primer_apellido
    i.primer_apellido,

    -- segundo_apellido
    i.segundo_apellido,

    -- sexo: Mapeo a cr_sexotipo (NOT NULL)
    COALESCE(
        (SELECT t_id FROM {schema}.cr_sexotipo WHERE ilicode = i.sexo LIMIT 1),
        (SELECT t_id FROM {schema}.cr_sexotipo WHERE ilicode = 'Sin_Determinar' LIMIT 1)
    ),

    -- autoreconocimientoetnico: Mapeo a cr_autoreconocimientoetnicotipo
    (SELECT t_id FROM {schema}.cr_autoreconocimientoetnicotipo WHERE ilicode = i.grupo_etnico LIMIT 1),

    -- autoreconocimientocampesino
    i.autoreconocimientocampesino::boolean,

    -- razon_social
    i.razon_social,

    -- nombre
    i.nombre,

    -- tipo_interesado: Mapeo a col_interesadotipo
    (SELECT t_id FROM {schema}.col_interesadotipo WHERE ilicode = i.tipo_interesado LIMIT 1),

    -- numero_documento
    i.numero_documento,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(i.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version (puede ser NULL)
    i.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(i.espacio_de_nombres, 'CR_INTERESADO'),

    -- local_id (NOT NULL)
    COALESCE(i.local_id, i.id::varchar)

FROM tmp_cr_interesado i;
