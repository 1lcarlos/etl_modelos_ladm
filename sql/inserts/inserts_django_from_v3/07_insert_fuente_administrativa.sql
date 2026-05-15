-- Inserción de fuentes administrativas en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- Mapeo de dominios: ilicode (interno_v3) → text_code (Django)
-- Tablas de dominio cambian de nombre:
--   col_fuenteadministrativatipo (v3) → gc_fuenteadministrativatipo (Django)
--   col_estadodisponibilidadtipo (v3 y Django, mismo nombre)
-- v3.nombre se mapea de vuelta a Django.numero_fuente
-- El id se genera automáticamente por la secuencia gc_fuenteadministrativa_id_seq de Django

INSERT INTO {schema}.gc_fuenteadministrativa (
    espacio_de_nombres,
    local_id,
    ente_emisor,
    fecha_documento_fuente,
    descripcion,
    numero_fuente,
    estado_disponibilidad,
    tipo
)
SELECT
    fa.espacio_de_nombres,
    fa.local_id,
    fa.ente_emisor,
    fa.fecha_documento_fuente::date,
    fa.descripcion,
    fa.numero_fuente,

    -- estado_disponibilidad (col_estadodisponibilidadtipo): ilicode → text_code
    COALESCE(
        (SELECT id FROM {schema}.col_estadodisponibilidadtipo WHERE text_code = fa.estado_disponibilidad LIMIT 1),
        (SELECT id FROM {schema}.col_estadodisponibilidadtipo WHERE fa.estado_disponibilidad ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS estado_disponibilidad,

    -- tipo (gc_fuenteadministrativatipo): ilicode de col_fuenteadministrativatipo → text_code
    COALESCE(
        (SELECT id FROM {schema}.gc_fuenteadministrativatipo WHERE text_code = fa.tipo_fuente_administrativa LIMIT 1),
        (SELECT id FROM {schema}.gc_fuenteadministrativatipo WHERE fa.tipo_fuente_administrativa ILIKE '%' || text_code || '%' LIMIT 1),
        (SELECT id FROM {schema}.gc_fuenteadministrativatipo WHERE text_code = 'Sin_Documento' LIMIT 1)
    ) AS tipo

FROM tmp_fuente_administrativa fa;
