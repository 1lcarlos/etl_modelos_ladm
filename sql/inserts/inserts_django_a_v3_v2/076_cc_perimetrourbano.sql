-- =============================================================================
-- Destino: cc_perimetrourbano (basket LimitesOperativosCatastro)   Origen: tmp_cc_perimetrourbano
-- Reglas:
--   * codigo_municipio: 3 ultimos digitos.
--   * zona = tipo_avaluo de Django (01 cabecera, 02.. corregimientos: codificacion de zona del NPN).
--   * nombre_geografico obligatorio, maximo 50 caracteres.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'cc_perimetrourbano', c.id, 'codigos/nombre/geometria',
       left(concat_ws(' | ', c.codigo_departamento, c.codigo_municipio, c.nombre_geografico), 255),
       'RECHAZADO', 'Codigos invalidos, nombre geografico vacio o mayor a 50 caracteres, o sin geometria'
FROM tmp_cc_perimetrourbano AS c
WHERE NOT c.es_centinela
  AND (etl_control.texto(c.codigo_departamento) !~ '^[0-9]{2}$'
       OR etl_control.texto(c.codigo_municipio) !~ '^[0-9]{3}([0-9]{2})?$'
       OR etl_control.texto(c.nombre_geografico) IS NULL
       OR length(etl_control.texto(c.nombre_geografico)) > 50
       OR c.geometria IS NULL);

INSERT INTO {schema}.cc_perimetrourbano (
    t_basket, t_ili_tid, codigo_departamento, codigo_municipio, zona, nombre_geografico, codigo_nombre, geometria
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro'),
    public.uuid_generate_v4()::text,
    etl_control.texto(c.codigo_departamento),
    right(etl_control.texto(c.codigo_municipio), 3),
    CASE WHEN length(etl_control.texto(c.tipo_avaluo)) <= 2 THEN etl_control.texto(c.tipo_avaluo) END,
    etl_control.texto(c.nombre_geografico),
    left(etl_control.texto(c.codigo_nombre), 255),
    c.geometria::geometry
FROM tmp_cc_perimetrourbano AS c
WHERE NOT c.es_centinela
  AND etl_control.texto(c.codigo_departamento) ~ '^[0-9]{2}$'
  AND etl_control.texto(c.codigo_municipio) ~ '^[0-9]{3}([0-9]{2})?$'
  AND length(etl_control.texto(c.nombre_geografico)) <= 50
  AND c.geometria IS NOT NULL;
