-- INSERT para tabla ric_terreno
-- Migra terrenos desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18

INSERT INTO ric.ric_terreno (
    t_id, t_ili_tid, area_terreno, area_digital_gestor, geometria,
    dimension, etiqueta, relacion_superficie, nivel,
    comienzo_vida_util_version, fin_vida_util_version, espacio_de_nombres, local_id
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    COALESCE(ST_Area(t.geometria)::numeric(15,1), 0),
    ST_Area(t.geometria)::numeric(15,1),
    CASE
        WHEN ST_GeometryType(t.geometria) = 'ST_MultiPolygon' THEN ST_Force3D(ST_Transform(t.geometria, 9377))
        WHEN ST_GeometryType(t.geometria) = 'ST_Polygon' THEN ST_Force3D(ST_Transform(ST_Multi(t.geometria), 9377))
        ELSE ST_Force3D(ST_Transform(ST_Multi(ST_MakeValid(t.geometria)), 9377))
    END,
    (SELECT t_id FROM ric.col_dimensiontipo WHERE ilicode = 'Dim2D' LIMIT 1),
    t.etiqueta,
    (SELECT t_id FROM ric.col_relacionsuperficietipo WHERE ilicode = 'En_Rasante' LIMIT 1),
    NULL,
    COALESCE(t.comienzo_vida_util_version::timestamp, NOW()),
    t.fin_vida_util_version::timestamp,
    COALESCE(t.espacio_de_nombres, 'RIC_TERRENO'),
    COALESCE(t.id::varchar, t.codigo)
FROM tmp_terreno t
WHERE t.geometria IS NOT NULL;
