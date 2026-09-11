-- Destino: extdireccion (estructura de gc_predio)
WITH datos AS (
    SELECT
        ex.id,
        ex.seq,
        ex.es_direccion_principal,
        ex.codigo_postal,
        ex.valor_via_principal,
        ex.letra_via_principal,
        ex.valor_via_generadora,
        ex.letra_via_generadora,
        ex.numero_predio,
        ex.complemento,
        ex.nombre_predio,
        ex.localizacion,
        ex.gc_predio_direccion,
        etd.text_code  AS tipo_direccion,
        ecvp.text_code AS clase_via_principal,
        esc.text_code  AS sector_ciudad,
        esp.text_code  AS sector_predio
    FROM {schema}.extdireccion AS ex
    LEFT JOIN {schema}.extdireccion_tipo_direccion AS etd ON etd.id = ex.tipo_direccion
    LEFT JOIN {schema}.extdireccion_clase_via_principal AS ecvp ON ecvp.id = ex.clase_via_principal
    LEFT JOIN {schema}.extdireccion_sector_ciudad AS esc ON esc.id = ex.sector_ciudad
    LEFT JOIN {schema}.extdireccion_sector_predio AS esp ON esp.id = ex.sector_predio
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
