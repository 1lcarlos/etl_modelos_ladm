-- =============================================================================
-- Destino: cc_sectorrural (basket LimitesOperativosCatastro)   Origen: tmp_cc_sectorrural
-- Longitud v3: codigo 9.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'cc_sectorrural', c.id, 'codigo/geometria', c.codigo,
       'RECHAZADO', 'Codigo vacio, excede 9 caracteres o sin geometria'
FROM tmp_cc_sectorrural AS c
WHERE NOT c.es_centinela
  AND (etl_control.texto(c.codigo) IS NULL OR length(etl_control.texto(c.codigo)) > 9 OR c.geometria IS NULL);

INSERT INTO {schema}.cc_sectorrural (t_basket, t_ili_tid, codigo, geometria)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro'),
    public.uuid_generate_v4()::text,
    etl_control.texto(c.codigo),
    c.geometria::geometry
FROM tmp_cc_sectorrural AS c
WHERE NOT c.es_centinela
  AND length(etl_control.texto(c.codigo)) <= 9
  AND c.geometria IS NOT NULL;
