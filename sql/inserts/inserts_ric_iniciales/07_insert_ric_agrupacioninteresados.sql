-- INSERT para tabla ric_agrupacioninteresados
-- Fecha: 2025-12-18

INSERT INTO ric.ric_agrupacioninteresados (
    t_id, t_ili_tid, tipo, nombre, comienzo_vida_util_version,
    fin_vida_util_version, espacio_de_nombres, local_id
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    COALESCE(
        (SELECT t_id FROM ric.col_grupointeresadotipo WHERE ilicode ILIKE '%' || a.tipo_agrupacion || '%' LIMIT 1),
        (SELECT t_id FROM ric.col_grupointeresadotipo WHERE ilicode = 'Grupo_Civil' LIMIT 1)
    ),
    a.nombre,
    COALESCE(a.comienzo_vida_util_version::timestamp, NOW()),
    NULL::timestamp,
    COALESCE(a.espacio_de_nombres, 'RIC_AGRUPACION'),
    COALESCE(a.local_id, a.id_agrupacion::varchar)
FROM tmp_agrupacion_interesados a;
