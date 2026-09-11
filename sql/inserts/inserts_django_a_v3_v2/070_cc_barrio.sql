-- =============================================================================
-- Destino: cc_barrio (basket LimitesOperativosCatastro)   Origen: tmp_cc_barrio
-- Se rechaza si codigo o nombre estan vacios, exceden la longitud de v3 (13 / 100)
-- o no hay geometria. No se trunca informacion.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'cc_barrio', c.id, 'codigo/nombre/geometria', left(concat_ws(' | ', c.codigo, c.nombre), 255),
       'RECHAZADO', 'Codigo o nombre vacio, excede longitud de v3 (codigo 13, nombre 100) o sin geometria'
FROM tmp_cc_barrio AS c
WHERE NOT c.es_centinela
  AND (etl_control.texto(c.codigo) IS NULL OR length(etl_control.texto(c.codigo)) > 13
       OR etl_control.texto(c.nombre) IS NULL OR length(etl_control.texto(c.nombre)) > 100
       OR c.geometria IS NULL);

INSERT INTO {schema}.cc_barrio (t_basket, t_ili_tid, codigo, nombre, geometria)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro'),
    public.uuid_generate_v4()::text,
    etl_control.texto(c.codigo),
    etl_control.texto(c.nombre),
    c.geometria::geometry
FROM tmp_cc_barrio AS c
WHERE NOT c.es_centinela
  AND length(etl_control.texto(c.codigo)) <= 13
  AND length(etl_control.texto(c.nombre)) <= 100
  AND c.geometria IS NOT NULL;
