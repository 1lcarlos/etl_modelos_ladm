-- Los valores de avalúo de intersección no existen en el modelo Django.
-- Las columnas avaluo_catastral_interseccion_terreno y _total son NOT NULL en destino,
-- por lo que no es posible insertar con NULLs.
-- Este insert permanece vacío hasta que los datos estén disponibles en el origen.
-- Si se agregan datos, vincular via gc_predio_informalidad.t_id en destino.
INSERT INTO {schema}.gc_estructuraavaluointerseccion (
    t_id,
    avaluo_catastral_interseccion_terreno,
    avaluo_catastral_interseccion_unidades_construccion,
    avaluo_catastral_interseccion_total,
    gc_predio_informalidad_avaluo_catastral_interseccion
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    tmp.avaluo_catastral_interseccion_terreno,
    tmp.avaluo_catastral_interseccion_uc,
    tmp.avaluo_catastral_interseccion_total,
    inf.t_id
FROM tmp_gc_estructuraavaluointerseccion tmp
JOIN {schema}.gc_predio_informalidad inf
    ON inf.t_id = tmp.informalidad_id
WHERE tmp.avaluo_catastral_interseccion_terreno IS NOT NULL
  AND tmp.avaluo_catastral_interseccion_total IS NOT NULL;
