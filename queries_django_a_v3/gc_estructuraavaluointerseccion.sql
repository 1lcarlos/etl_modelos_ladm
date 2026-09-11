-- Avalúo catastral de intersección para predios de informalidad
-- Nota: avalúos de intersección no existen en Django; esta tabla queda vacía.
-- Si en el futuro se agregan, vincular via gc_predio_informalidad.id → gc_predio_informalidad destino.
SELECT
    NULL::numeric   AS avaluo_catastral_interseccion_terreno,
    NULL::numeric   AS avaluo_catastral_interseccion_uc,
    NULL::numeric   AS avaluo_catastral_interseccion_total,
    inf.id          AS informalidad_id
FROM {schema}.gc_predio_informalidad inf
WHERE FALSE;  -- sin datos disponibles; columnas NOT NULL impiden insertar NULLs
