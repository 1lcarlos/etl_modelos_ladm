-- INSERT para tabla gc_fuenteadministrativa (Modelo Interno Django)
-- Migra fuentes administrativas desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_fuenteadministrativa (query cca_fuenteadministrativa.sql)
-- Destino: gc_fuenteadministrativa (modelo interno Django)
--
-- Diferencias clave con modelo CCA:
--   - PK es 'id' (se usa cca_fuenteadministrativa_id)
--   - Dominios usan text_code (no ilicode) y FK apunta a 'id' (no t_id)
--   - CCA observacion -> Django descripcion
--   - Django tiene valor_transaccion y estado_disponibilidad que no existen en CCA (NULL)
--
-- Dependencias:
--   - Las tablas de dominio deben estar pobladas
--   - No depende de otras tablas de datos
--
-- IMPORTANTE: Este insert puede ejecutarse en paralelo con gc_derecho

INSERT INTO {schema}.gc_fuenteadministrativa (
    id,
    espacio_de_nombres,
    local_id,
    ente_emisor,
    fecha_documento_fuente,
    descripcion,
    numero_fuente,
    valor_transaccion,
    estado_disponibilidad,
    tipo
)
SELECT
    -- id: Usar cca_fuenteadministrativa_id como id en Django
    f.cca_fuenteadministrativa_id,

    -- espacio_de_nombres
    'GC_FUENTEADMINISTRATIVA_CCA',

    -- local_id
    f.cca_fuenteadministrativa_id::varchar,

    -- ente_emisor
    f.ente_emisor,

    -- fecha_documento_fuente
    f.fecha_documento_fuente::date,

    -- descripcion: CCA observacion -> Django descripcion
    f.observacion,

    -- numero_fuente
    f.numero_fuente,

    -- valor_transaccion: No existe en CCA
    NULL,

    -- estado_disponibilidad: No existe en CCA
    NULL,

    -- tipo: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_fuenteadministrativatipo
         WHERE text_code = f.tipo LIMIT 1),
        (SELECT id FROM {schema}.gc_fuenteadministrativatipo
         WHERE text_code ILIKE '%' || f.tipo || '%' LIMIT 1),
        NULL
    )

FROM tmp_cca_fuenteadministrativa f;
