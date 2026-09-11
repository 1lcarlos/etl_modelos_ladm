-- =============================================================================
-- 000 - Preparacion del destino (primer archivo del orden, se ejecuta por esquema)
--   1. Esquema etl_control (fuera del modelo LADM): incidencias, conciliacion,
--      mapeo de dominios y funciones auxiliares que usan los inserts.
--   2. Dataset y baskets de ili2db. El destino usa BasketHandling=readWrite y
--      t_basket es obligatorio en todas las clases.
--
-- Las tablas de etl_control se escriben entre comillas dobles cuando se insertan
-- datos ("etl_control"."incidencia"). El motor decide que tablas truncar leyendo
-- los nombres sin comillas que siguen a la palabra clave de insercion; con comillas
-- no las trunca, y asi las incidencias de un esquema no se borran al procesar otro.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS etl_control;

CREATE TABLE IF NOT EXISTS etl_control.incidencia (
    id            bigserial PRIMARY KEY,
    esquema       text      NOT NULL,
    tabla_destino text      NOT NULL,
    id_origen     text,               -- id de Django (NULL en incidencias agregadas)
    campo         text,
    valor_origen  text,
    accion        text      NOT NULL, -- RECHAZADO | NULIFICADO | DERIVADO | NO_MIGRADO | ADVERTENCIA
    motivo        text      NOT NULL,
    fecha         timestamp NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS incidencia_esquema_tabla_idx ON etl_control.incidencia (esquema, tabla_destino, accion);

CREATE TABLE IF NOT EXISTS etl_control.conciliacion (
    esquema       text      NOT NULL,
    tabla_destino text      NOT NULL,
    filas_origen  bigint,
    filas_destino bigint,
    rechazadas    bigint,
    fecha         timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (esquema, tabla_destino)
);

CREATE TABLE IF NOT EXISTS etl_control.mapeo_dominio (
    dominio         text NOT NULL,  -- tabla de dominio del destino v3
    valor_origen    text NOT NULL,  -- text_code (o texto libre) de Django
    ilicode_destino text,           -- NULL = sin equivalente aprobado: queda NULL + incidencia
    observacion     text,
    PRIMARY KEY (dominio, valor_origen)
);

DELETE FROM etl_control.incidencia WHERE esquema = '{schema}';
DELETE FROM etl_control.conciliacion WHERE esquema = '{schema}';

-- Normaliza texto: recorta, colapsa espacios internos y convierte '' en NULL.
CREATE OR REPLACE FUNCTION etl_control.texto(p_valor text)
RETURNS text LANGUAGE sql IMMUTABLE AS $fn$
    SELECT NULLIF(btrim(regexp_replace(p_valor, '\s+', ' ', 'g')), '')
$fn$;

-- Matricula inmobiliaria valida para v3 (bigint entre 1 y 9.999.999.999); si no, NULL.
CREATE OR REPLACE FUNCTION etl_control.matricula(p_valor text)
RETURNS bigint LANGUAGE sql IMMUTABLE AS $fn$
    SELECT CASE
        WHEN btrim(p_valor) ~ '^[0-9]{1,10}$'
         AND btrim(p_valor)::bigint BETWEEN 1 AND 9999999999
        THEN btrim(p_valor)::bigint
    END
$fn$;

-- Traduce un valor de Django al t_id del dominio destino.
--   1. Si el valor esta en etl_control.mapeo_dominio se usa su ilicode_destino
--      (NULL en el mapeo = sin equivalente: devuelve NULL).
--   2. Si no esta en el mapeo, se busca el mismo valor como ilicode.
--   Si el ilicode existe en la enumeracion base y en la extendida, se prefiere la
--   extendida (baseclass no nulo), como hacia el flujo original.
CREATE OR REPLACE FUNCTION etl_control.dominio(p_esquema text, p_tabla text, p_valor text)
RETURNS bigint LANGUAGE plpgsql STABLE AS $fn$
DECLARE
    v_ilicode text;
    v_t_id    bigint;
BEGIN
    IF etl_control.texto(p_valor) IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT m.ilicode_destino
      INTO v_ilicode
      FROM etl_control.mapeo_dominio AS m
     WHERE m.dominio = p_tabla
       AND m.valor_origen = p_valor;
    IF NOT FOUND THEN
        v_ilicode := p_valor;
    END IF;
    IF v_ilicode IS NULL THEN
        RETURN NULL;
    END IF;

    EXECUTE format('SELECT t_id FROM %I.%I WHERE ilicode = $1 ORDER BY (baseclass IS NULL), t_id LIMIT 1',
                   p_esquema, p_tabla)
       INTO v_t_id
      USING v_ilicode;
    RETURN v_t_id;
END
$fn$;

-- Dataset y baskets (idempotente). Topics usados:
--   Modelo_Interno_ACC_V3.Modelo_Interno            -> gc_*, col_*, ext*
--   Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro -> cc_*
INSERT INTO {schema}.t_ili2db_dataset (t_id, datasetname)
SELECT nextval('{schema}.t_ili2db_seq'::regclass), 'Baseline'
WHERE NOT EXISTS (SELECT 1 FROM {schema}.t_ili2db_dataset WHERE datasetname = 'Baseline');

INSERT INTO {schema}.t_ili2db_basket (t_id, dataset, topic, t_ili_tid, attachmentkey)
SELECT nextval('{schema}.t_ili2db_seq'::regclass), d.t_id, t.topic,
       public.uuid_generate_v4()::text, 'etl_django_a_v3'
FROM {schema}.t_ili2db_dataset AS d
CROSS JOIN (VALUES
    ('Modelo_Interno_ACC_V3.Modelo_Interno'),
    ('Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro')
) AS t(topic)
WHERE d.datasetname = 'Baseline'
  AND NOT EXISTS (SELECT 1 FROM {schema}.t_ili2db_basket AS b WHERE b.topic = t.topic);
