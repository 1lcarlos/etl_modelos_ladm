-- =============================================================================
-- Destino: cc_manzana (basket LimitesOperativosCatastro)   Origen: tmp_cc_manzana
-- Longitudes v3: codigo 17 (obligatorio), nombre 50 (opcional: si excede -> NULL).
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'cc_manzana', c.id, 'codigo/geometria', c.codigo,
       'RECHAZADO', 'Codigo vacio, excede 17 caracteres o sin geometria'
FROM tmp_cc_manzana AS c
WHERE NOT c.es_centinela
  AND (etl_control.texto(c.codigo) IS NULL OR length(etl_control.texto(c.codigo)) > 17 OR c.geometria IS NULL)
UNION ALL
SELECT '{schema}', 'cc_manzana', c.id, 'nombre', c.nombre, 'NULIFICADO', 'Excede 50 caracteres'
FROM tmp_cc_manzana AS c
WHERE NOT c.es_centinela AND length(etl_control.texto(c.nombre)) > 50;

INSERT INTO {schema}.cc_manzana (t_basket, t_ili_tid, codigo, nombre, geometria)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro'),
    public.uuid_generate_v4()::text,
    etl_control.texto(c.codigo),
    CASE WHEN length(etl_control.texto(c.nombre)) <= 50 THEN etl_control.texto(c.nombre) END,
    c.geometria::geometry
FROM tmp_cc_manzana AS c
WHERE NOT c.es_centinela
  AND length(etl_control.texto(c.codigo)) <= 17
  AND c.geometria IS NOT NULL;
