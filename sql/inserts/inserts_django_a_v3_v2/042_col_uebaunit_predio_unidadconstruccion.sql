-- =============================================================================
-- Destino: col_uebaunit (unidad de construccion -> predio)
-- Origen : tmp_col_uebaunit_predio_unidadconstruccion
-- Cascada: DIRECTO -> VIA_CONSTRUCCION -> NPN_LOCAL_ID. SIN_ENLACE no genera relacion.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT DISTINCT '{schema}', 'col_uebaunit_unidadconstruccion', u.id_uc, 'baunit', NULL::text, 'ADVERTENCIA',
       'Unidad sin predio en origen (sin enlace directo, por construccion ni por NPN): se migra sin relacion'
FROM tmp_col_uebaunit_predio_unidadconstruccion AS u
WHERE NOT u.es_centinela AND u.metodo_enlace = 'SIN_ENLACE'
UNION ALL
SELECT DISTINCT '{schema}', 'col_uebaunit_unidadconstruccion', u.id_uc, 'baunit', u.id_predio_npn, 'ADVERTENCIA',
       'El NPN de la unidad (local_id) corresponde a un predio distinto del enlace formal; se usa el enlace formal'
FROM tmp_col_uebaunit_predio_unidadconstruccion AS u
WHERE NOT u.es_centinela AND u.contradice_npn::boolean AND u.metodo_enlace IN ('DIRECTO', 'VIA_CONSTRUCCION')
UNION ALL
SELECT '{schema}', 'col_uebaunit_unidadconstruccion', NULL, 'baunit',
       count(DISTINCT u.id_uc)::text || ' unidades', 'DERIVADO',
       'Enlace unidad-predio a traves de gc_construccion (sin enlace directo en origen)'
FROM tmp_col_uebaunit_predio_unidadconstruccion AS u
WHERE NOT u.es_centinela AND u.metodo_enlace = 'VIA_CONSTRUCCION'
HAVING count(*) > 0
UNION ALL
SELECT '{schema}', 'col_uebaunit_unidadconstruccion', NULL, 'baunit',
       count(DISTINCT u.id_uc)::text || ' unidades', 'DERIVADO',
       'Enlace unidad-predio por NPN en local_id (sin enlace directo ni por construccion)'
FROM tmp_col_uebaunit_predio_unidadconstruccion AS u
WHERE NOT u.es_centinela AND u.metodo_enlace = 'NPN_LOCAL_ID'
HAVING count(*) > 0
UNION ALL
SELECT '{schema}', 'col_uebaunit_unidadconstruccion', u.id_uc, 'baunit', u.id_predio, 'RECHAZADO',
       'Unidad o predio no migrado'
FROM tmp_col_uebaunit_predio_unidadconstruccion AS u
LEFT JOIN {schema}.gc_unidadconstruccion AS uc
       ON uc.local_id = u.id_uc AND uc.espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'
LEFT JOIN {schema}.gc_predio AS p
       ON p.local_id = u.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT u.es_centinela AND u.id_predio IS NOT NULL AND (uc.t_id IS NULL OR p.t_id IS NULL);

INSERT INTO {schema}.col_uebaunit (t_basket, ue_gc_unidadconstruccion, baunit)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    e.id_uc_destino,
    e.id_predio_destino
FROM (
    SELECT DISTINCT uc.t_id AS id_uc_destino, p.t_id AS id_predio_destino
    FROM tmp_col_uebaunit_predio_unidadconstruccion AS u
    JOIN {schema}.gc_unidadconstruccion AS uc
      ON uc.local_id = u.id_uc AND uc.espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'
    JOIN {schema}.gc_predio AS p
      ON p.local_id = u.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
    WHERE NOT u.es_centinela
) AS e;
