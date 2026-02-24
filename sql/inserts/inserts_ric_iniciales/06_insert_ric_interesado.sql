-- INSERT para tabla ric_interesado
-- Fecha: 2025-12-18

INSERT INTO ric.ric_interesado (
    t_id, t_ili_tid, tipo, tipo_documento, documento_identidad,
    primer_nombre, segundo_nombre, primer_apellido, segundo_apellido,
    sexo, grupoetnico, razon_social, estado_civil, nombre,
    comienzo_vida_util_version, fin_vida_util_version, espacio_de_nombres, local_id
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    COALESCE(
        (SELECT t_id FROM ric.ric_interesadotipo WHERE ilicode ILIKE '%' || i.interesadotipo || '%' LIMIT 1),
        (SELECT t_id FROM ric.ric_interesadotipo WHERE ilicode = 'Persona_Natural' LIMIT 1)
    ),
    COALESCE(
        (SELECT t_id FROM ric.ric_interesadodocumentotipo WHERE ilicode ILIKE '%' || i.tipo_documento || '%' LIMIT 1),
        (SELECT t_id FROM ric.ric_interesadodocumentotipo WHERE ilicode = 'Cedula_Ciudadania' LIMIT 1)
    ),
    COALESCE(i.documento_identidad, 'SIN_DOCUMENTO'),
    i.primer_nombre, i.segundo_nombre, i.primer_apellido, i.segundo_apellido,
    (SELECT t_id FROM ric.ric_sexotipo WHERE ilicode ILIKE '%' || i.sexo || '%' LIMIT 1),
    (SELECT t_id FROM ric.ric_grupoetnicotipo WHERE ilicode ILIKE '%' || i.grupo_etnico || '%' LIMIT 1),
    i.razon_social,
    (SELECT t_id FROM ric.ric_estadociviltipo WHERE ilicode ILIKE '%' || i.estado_civil || '%' LIMIT 1),
    i.nombre,
    COALESCE(i.comienzo_vida_util_version::timestamp, NOW()),
    NULL::timestamp,
    COALESCE(i.espacio_de_nombres, 'RIC_INTERESADO'),
    COALESCE(i.local_id, i.id_interesado::varchar)
FROM tmp_interesado i
WHERE i.documento_identidad IS NOT NULL;
