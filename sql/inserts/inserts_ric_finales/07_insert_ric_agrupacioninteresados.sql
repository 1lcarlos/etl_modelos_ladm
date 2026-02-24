-- INSERT para tabla ric_agrupacioninteresados
-- Migra agrupaciones de interesados desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Usa datos de tmp_agrupacion_interesados (query agrupacion_interesados.sql)

INSERT INTO ric.ric_agrupacioninteresados (
    t_id,
    t_ili_tid,
    tipo,
    nombre,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    a.id_agrupacion::bigint,
    uuid_generate_v4(),

    -- tipo: Mapeo a col_grupointeresadotipo
    COALESCE(
        (SELECT t_id FROM ric.col_grupointeresadotipo
         WHERE ilicode ILIKE '%' || a.tipo_agrupacion || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.col_grupointeresadotipo
         WHERE ilicode = 'Grupo_Civil'
         LIMIT 1)
    ),

    -- nombre
    a.nombre,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(a.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version
    NULL::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(a.espacio_de_nombres, 'RIC_AGRUPACION'),

    -- local_id (NOT NULL)
    COALESCE(a.id_agrupacion::varchar, a.local_id)

FROM tmp_agrupacion_interesados a;
