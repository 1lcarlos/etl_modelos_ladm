-- INSERT para tabla gc_agrupacioninteresados (Modelo Interno Django)
-- Migra agrupaciones de interesados desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_agrupacioninteresados (query cca_agrupacioninteresados.sql)
-- Destino: gc_agrupacioninteresados (modelo interno Django)
--
-- Diferencias clave con modelo CCA:
--   - PK es 'id' (se usa cca_agrupacion_id)
--   - CCA referencia cca_grupointeresadotipo, Django referencia col_grupointeresadotipo
--   - Dominios usan text_code (no ilicode) y FK apunta a 'id' (no t_id)
--
-- Dependencias:
--   - Las tablas de dominio (col_grupointeresadotipo) deben estar pobladas
--   - No depende de gc_interesado (se puede ejecutar en paralelo)
--
-- IMPORTANTE: Este insert puede ejecutarse en paralelo con gc_interesado

INSERT INTO {schema}.gc_agrupacioninteresados (
    id,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    nombre,
    tipo
)
SELECT
    -- id: Usar cca_agrupacion_id como id en Django
    a.cca_agrupacion_id,

    -- espacio_de_nombres
    'GC_AGRUPACIONINTERESADOS_CCA',

    -- local_id
    a.cca_agrupacion_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- nombre
    a.nombre,

    -- tipo: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.col_grupointeresadotipo
         WHERE text_code = a.tipo LIMIT 1),
        (SELECT id FROM {schema}.col_grupointeresadotipo
         WHERE text_code ILIKE '%' || a.tipo || '%' LIMIT 1),
        NULL
    )

FROM tmp_cca_agrupacioninteresados a;
