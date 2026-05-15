-- Inserción de extdireccion en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- Mapeo de dominios: ilicode (interno_v3) → text_code (Django)
-- Las tablas de dominio de extdireccion tienen el mismo nombre en ambos modelos
-- El FK gc_predio_direccion se resuelve mediante join con local_id
-- El id se genera automáticamente por la secuencia extdireccion_id_seq de Django

INSERT INTO {schema}.extdireccion (
    tipo_direccion,
    es_direccion_principal,
    localizacion,
    codigo_postal,
    clase_via_principal,
    valor_via_principal,
    letra_via_principal,
    sector_ciudad,
    valor_via_generadora,
    letra_via_generadora,
    numero_predio,
    sector_predio,
    complemento,
    nombre_predio,
    gc_predio_direccion
)
SELECT
    -- tipo_direccion: ilicode → text_code, fallback 'Estructurada'
    COALESCE(
        (SELECT id FROM {schema}.extdireccion_tipo_direccion WHERE text_code = te.tipo_direccion LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_tipo_direccion WHERE te.tipo_direccion ILIKE '%' || text_code || '%' LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_tipo_direccion WHERE text_code = 'Estructurada' LIMIT 1)
    ) AS tipo_direccion,

    te.es_direccion_principal::boolean,
    te.localizacion,
    te.codigo_postal,

    -- clase_via_principal: ilicode → text_code (puede ser NULL)
    COALESCE(
        (SELECT id FROM {schema}.extdireccion_clase_via_principal WHERE text_code = te.clase_via_principal LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_clase_via_principal WHERE te.clase_via_principal ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS clase_via_principal,

    te.valor_via_principal,
    te.letra_via_principal,

    -- sector_ciudad: ilicode → text_code (puede ser NULL)
    COALESCE(
        (SELECT id FROM {schema}.extdireccion_sector_ciudad WHERE text_code = te.sector_ciudad LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_sector_ciudad WHERE te.sector_ciudad ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS sector_ciudad,

    te.valor_via_generadora,
    te.letra_via_generadora,
    te.numero_predio,

    -- sector_predio: ilicode → text_code (puede ser NULL)
    COALESCE(
        (SELECT id FROM {schema}.extdireccion_sector_predio WHERE text_code = te.sector_predio LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_sector_predio WHERE te.sector_predio ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS sector_predio,

    te.complemento,
    te.nombre_predio,

    -- gc_predio_direccion: FK resuelta por local_id
    p.id AS gc_predio_direccion

FROM tmp_extdireccion te
LEFT JOIN {schema}.gc_predio p ON p.local_id = te.gc_predio_direccion;
