-- =============================================================================
-- Destino: cc_localidadcomuna (basket LimitesOperativosCatastro)   Origen: tmp_cc_localidadcomuna
-- Longitudes v3: codigo 11, nombre 50.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'cc_localidadcomuna', c.id, 'codigo/nombre/geometria', left(concat_ws(' | ', c.codigo, c.nombre), 255),
       'RECHAZADO', 'Codigo o nombre vacio, excede longitud de v3 (codigo 11, nombre 50) o sin geometria'
FROM tmp_cc_localidadcomuna AS c
WHERE NOT c.es_centinela
  AND (etl_control.texto(c.codigo) IS NULL OR length(etl_control.texto(c.codigo)) > 11
       OR etl_control.texto(c.nombre) IS NULL OR length(etl_control.texto(c.nombre)) > 50
       OR c.geometria IS NULL);

INSERT INTO {schema}.cc_localidadcomuna (t_basket, t_ili_tid, codigo, nombre, geometria)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro'),
    public.uuid_generate_v4()::text,
    etl_control.texto(c.codigo),
    etl_control.texto(c.nombre),
    c.geometria::geometry
FROM tmp_cc_localidadcomuna AS c
WHERE NOT c.es_centinela
  AND length(etl_control.texto(c.codigo)) <= 11
  AND length(etl_control.texto(c.nombre)) <= 50
  AND c.geometria IS NOT NULL;
