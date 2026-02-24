-- INSERT para tabla ric_predio
-- Migra predios desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18

INSERT INTO ric.ric_predio (
    t_id, t_ili_tid, departamento, municipio, codigo_homologado, nupre,
    codigo_orip, matricula_inmobiliaria, numero_predial, numero_predial_anterior,
    fecha_inscripcion_catastral, condicion_predio, destinacion_economica, tipo,
    avaluo_catastral, zona, vigencia_actualizacion_catastral, estado, catastro,
    ric_gestorcatastral, ric_operadorcatastral, nombre, comienzo_vida_util_version,
    fin_vida_util_version, espacio_de_nombres, local_id
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    COALESCE(p.departamento, '00'),
    COALESCE(p.municipio, '000'),
    COALESCE(NULLIF(p.nupre, 'BBK00000'), SUBSTRING(p.numero_predial_nacional FROM 1 FOR 11)),
    p.nupre,
    p.codigo_orip,
    p.matricula_inmobiliaria::varchar,
    p.numero_predial_nacional,
    p.numero_predial_anterior,
    NULL::date,
    COALESCE(
        (SELECT t_id FROM ric.ric_condicionprediotipo WHERE ilicode = p.condicion_predio LIMIT 1),
        (SELECT t_id FROM ric.ric_condicionprediotipo WHERE ilicode = 'NPH' LIMIT 1)
    ),
    COALESCE(
        (SELECT t_id FROM ric.ric_destinacioneconomicatipo WHERE ilicode = p.destinacion_economica LIMIT 1),
        CASE WHEN SUBSTRING(p.numero_predial_nacional FROM 6 FOR 2) = '00'
            THEN (SELECT t_id FROM ric.ric_destinacioneconomicatipo WHERE ilicode = 'Agricola' LIMIT 1)
            ELSE (SELECT t_id FROM ric.ric_destinacioneconomicatipo WHERE ilicode = 'Habitacional' LIMIT 1)
        END
    ),
    (SELECT t_id FROM ric.col_unidadadministrativabasicatipo WHERE ilicode ILIKE '%Predio%' LIMIT 1),
    COALESCE(p.area_catastral_terreno::numeric * 1000, 0),
    COALESCE(
        (SELECT t_id FROM ric.ric_zonatipo WHERE ilicode = p.clase_suelo LIMIT 1),
        CASE WHEN SUBSTRING(p.numero_predial_nacional FROM 6 FOR 2) = '00'
            THEN (SELECT t_id FROM ric.ric_zonatipo WHERE ilicode = 'Rural' LIMIT 1)
            ELSE (SELECT t_id FROM ric.ric_zonatipo WHERE ilicode = 'Urbana' LIMIT 1)
        END
    ),
    COALESCE(p.vigencia_actualizacion_catastral::date, CURRENT_DATE),
    COALESCE(
        (SELECT t_id FROM ric.ric_estadotipo WHERE ilicode = p.estado LIMIT 1),
        (SELECT t_id FROM ric.ric_estadotipo WHERE ilicode = 'Activo' LIMIT 1)
    ),
    NULL,
    (SELECT t_id FROM ric.ric_gestorcatastral WHERE nombre_gestor = 'IGAC' LIMIT 1),
    (SELECT t_id FROM ric.ric_operadorcatastral WHERE nombre_operador = 'IGAC' LIMIT 1),
    p.nombre,
    COALESCE(p.comienzo_vida_util_version::timestamp, NOW()),
    p.fin_vida_util_version::timestamp,
    COALESCE(p.espacio_de_nombres, 'RIC_PREDIO'),
    COALESCE(p.id::varchar, p.numero_predial_nacional)
FROM tmp_predio p;
