-- =============================================================================
-- Destino: cc_limitemunicipio (basket LimitesOperativosCatastro)   Origen: tmp_cc_limitemunicipio
-- codigo_municipio: v3 admite 3 caracteres; se toman los 3 ultimos (sirve para '123' y '25123').
-- (El flujo original usaba substring(codigo, 3, 3), que convierte '123' en '3'.)
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'cc_limitemunicipio', c.id, 'codigos/nombre/geometria',
       left(concat_ws(' | ', c.codigo_departamento, c.codigo_municipio, c.nombre_municipio), 255),
       'RECHAZADO', 'Codigo de departamento o municipio invalido, nombre vacio o sin geometria'
FROM tmp_cc_limitemunicipio AS c
WHERE NOT c.es_centinela
  AND (etl_control.texto(c.codigo_departamento) !~ '^[0-9]{2}$'
       OR etl_control.texto(c.codigo_municipio) !~ '^[0-9]{3}([0-9]{2})?$'
       OR etl_control.texto(c.nombre_municipio) IS NULL
       OR c.geometria IS NULL);

INSERT INTO {schema}.cc_limitemunicipio (t_basket, t_ili_tid, codigo_departamento, codigo_municipio, nombre_municipio, geometria)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro'),
    public.uuid_generate_v4()::text,
    etl_control.texto(c.codigo_departamento),
    right(etl_control.texto(c.codigo_municipio), 3),
    left(etl_control.texto(c.nombre_municipio), 255),
    c.geometria::geometry
FROM tmp_cc_limitemunicipio AS c
WHERE NOT c.es_centinela
  AND etl_control.texto(c.codigo_departamento) ~ '^[0-9]{2}$'
  AND etl_control.texto(c.codigo_municipio) ~ '^[0-9]{3}([0-9]{2})?$'
  AND etl_control.texto(c.nombre_municipio) IS NOT NULL
  AND c.geometria IS NOT NULL;
