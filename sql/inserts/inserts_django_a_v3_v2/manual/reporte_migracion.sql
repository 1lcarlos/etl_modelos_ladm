-- =============================================================================
-- Reporte de migracion Django -> v3 (ejecutar a mano en la BD destino)
-- Reemplazar cun25123 por el esquema a revisar.
-- =============================================================================

-- 1. Conciliacion por tabla
--    diferencia = origen - destino - rechazadas. Distinto de 0 indica filas que no
--    se cargaron sin incidencia de rechazo (revisar la regla del insert).
SELECT tabla_destino, filas_origen, filas_destino, rechazadas,
       filas_origen - filas_destino - rechazadas AS diferencia
FROM etl_control.conciliacion
WHERE esquema = 'cun25123'
ORDER BY (filas_origen - filas_destino - rechazadas) <> 0 DESC, tabla_destino;

-- 2. Resumen de incidencias
SELECT tabla_destino, accion, campo, motivo, count(*) AS registros
FROM etl_control.incidencia
WHERE esquema = 'cun25123'
GROUP BY tabla_destino, accion, campo, motivo
ORDER BY tabla_destino, accion, registros DESC;

-- 3. Mapeos de dominio pendientes de decision
SELECT dominio, valor_origen, observacion
FROM etl_control.mapeo_dominio
WHERE ilicode_destino IS NULL
ORDER BY dominio, valor_origen;

-- 4. Deuda de datos: atributos obligatorios del modelo que quedaron en NULL
--    (columnas relajadas en 002_relajar_obligatorios.sql)
SELECT tabla, columna, nulos, total
FROM (
    SELECT 'gc_predio' AS tabla, 'tipo_predio' AS columna, count(*) FILTER (WHERE tipo_predio IS NULL) AS nulos, count(*) AS total FROM cun25123.gc_predio
    UNION ALL SELECT 'gc_predio', 'condicion_predio', count(*) FILTER (WHERE condicion_predio IS NULL), count(*) FROM cun25123.gc_predio
    UNION ALL SELECT 'gc_predio', 'destinacion_economica', count(*) FILTER (WHERE destinacion_economica IS NULL), count(*) FROM cun25123.gc_predio
    UNION ALL SELECT 'gc_predio', 'area_catastral_terreno', count(*) FILTER (WHERE area_catastral_terreno IS NULL), count(*) FROM cun25123.gc_predio
    UNION ALL SELECT 'gc_predio', 'vigencia_actualizacion_catastral', count(*) FILTER (WHERE vigencia_actualizacion_catastral IS NULL), count(*) FROM cun25123.gc_predio
    UNION ALL SELECT 'gc_predio', 'clase_suelo', count(*) FILTER (WHERE clase_suelo IS NULL), count(*) FROM cun25123.gc_predio
    UNION ALL SELECT 'gc_predio', 'codigo_homologado', count(*) FILTER (WHERE codigo_homologado IS NULL), count(*) FROM cun25123.gc_predio
    UNION ALL SELECT 'gc_interesado', 'tipo_documento', count(*) FILTER (WHERE tipo_documento IS NULL), count(*) FROM cun25123.gc_interesado
    UNION ALL SELECT 'gc_interesado', 'sexo', count(*) FILTER (WHERE sexo IS NULL), count(*) FROM cun25123.gc_interesado
    UNION ALL SELECT 'gc_agrupacioninteresados', 'tipo', count(*) FILTER (WHERE tipo IS NULL), count(*) FROM cun25123.gc_agrupacioninteresados
    UNION ALL SELECT 'gc_derechocatastral', 'tipo', count(*) FILTER (WHERE tipo IS NULL), count(*) FROM cun25123.gc_derechocatastral
    UNION ALL SELECT 'gc_fuenteadministrativa', 'tipo', count(*) FILTER (WHERE tipo IS NULL), count(*) FROM cun25123.gc_fuenteadministrativa
    UNION ALL SELECT 'extdireccion', 'tipo_direccion', count(*) FILTER (WHERE tipo_direccion IS NULL), count(*) FROM cun25123.extdireccion
    UNION ALL SELECT 'gc_caracteristicasunidadconstruccion', 'tipo_unidad_construccion', count(*) FILTER (WHERE tipo_unidad_construccion IS NULL), count(*) FROM cun25123.gc_caracteristicasunidadconstruccion
    UNION ALL SELECT 'gc_caracteristicasunidadconstruccion', 'total_plantas', count(*) FILTER (WHERE total_plantas IS NULL), count(*) FROM cun25123.gc_caracteristicasunidadconstruccion
    UNION ALL SELECT 'gc_caracteristicasunidadconstruccion', 'uso', count(*) FILTER (WHERE uso IS NULL), count(*) FROM cun25123.gc_caracteristicasunidadconstruccion
    UNION ALL SELECT 'gc_caracteristicasunidadconstruccion', 'anio_construccion', count(*) FILTER (WHERE anio_construccion IS NULL), count(*) FROM cun25123.gc_caracteristicasunidadconstruccion
    UNION ALL SELECT 'gc_caracteristicasunidadconstruccion', 'area_construida', count(*) FILTER (WHERE area_construida IS NULL), count(*) FROM cun25123.gc_caracteristicasunidadconstruccion
    UNION ALL SELECT 'gc_caracteristicasunidadconstruccion', 'estado_conservacion', count(*) FILTER (WHERE estado_conservacion IS NULL), count(*) FROM cun25123.gc_caracteristicasunidadconstruccion
    UNION ALL SELECT 'gc_unidadconstruccion', 'tipo_planta', count(*) FILTER (WHERE tipo_planta IS NULL), count(*) FROM cun25123.gc_unidadconstruccion
    UNION ALL SELECT 'gc_unidadconstruccion', 'planta_ubicacion', count(*) FILTER (WHERE planta_ubicacion IS NULL), count(*) FROM cun25123.gc_unidadconstruccion
    UNION ALL SELECT 'gc_unidadconstruccion', 'cr_caracteristicasunidadconstruccion', count(*) FILTER (WHERE cr_caracteristicasunidadconstruccion IS NULL), count(*) FROM cun25123.gc_unidadconstruccion
    UNION ALL SELECT 'gc_estructuraavaluo', 'fecha_avaluo', count(*) FILTER (WHERE fecha_avaluo IS NULL), count(*) FROM cun25123.gc_estructuraavaluo
    UNION ALL SELECT 'gc_estructuraavaluo', 'avaluo_catastral', count(*) FILTER (WHERE avaluo_catastral IS NULL), count(*) FROM cun25123.gc_estructuraavaluo
    UNION ALL SELECT 'gc_puntolindero', 'puntotipo', count(*) FILTER (WHERE puntotipo IS NULL), count(*) FROM cun25123.gc_puntolindero
    UNION ALL SELECT 'gc_puntolindero', 'desacuerdo', count(*) FILTER (WHERE desacuerdo IS NULL), count(*) FROM cun25123.gc_puntolindero
    UNION ALL SELECT 'gc_puntolindero', 'metodoproduccion', count(*) FILTER (WHERE metodoproduccion IS NULL), count(*) FROM cun25123.gc_puntolindero
    UNION ALL SELECT 'gc_puntocontrol', 'puntotipo', count(*) FILTER (WHERE puntotipo IS NULL), count(*) FROM cun25123.gc_puntocontrol
    UNION ALL SELECT 'gc_puntocontrol', 'metodoproduccion', count(*) FILTER (WHERE metodoproduccion IS NULL), count(*) FROM cun25123.gc_puntocontrol
    UNION ALL SELECT 'gc_fuenteespacial', 'tipo', count(*) FILTER (WHERE tipo IS NULL), count(*) FROM cun25123.gc_fuenteespacial
    UNION ALL SELECT 'gc_predio_copropiedad', 'coeficiente', count(*) FILTER (WHERE coeficiente IS NULL), count(*) FROM cun25123.gc_predio_copropiedad
    UNION ALL SELECT 'gc_predio_copropiedad', 'area_catastral_terreno_coeficiente', count(*) FILTER (WHERE area_catastral_terreno_coeficiente IS NULL), count(*) FROM cun25123.gc_predio_copropiedad
    UNION ALL SELECT 'gc_predio_informalidad', 'area_terreno_interseccion', count(*) FILTER (WHERE area_terreno_interseccion IS NULL), count(*) FROM cun25123.gc_predio_informalidad
    UNION ALL SELECT 'gc_estructuraalertapredio', 'descripcion_alerta', count(*) FILTER (WHERE descripcion_alerta IS NULL), count(*) FROM cun25123.gc_estructuraalertapredio
    UNION ALL SELECT 'gc_estructuraalertapredio', 'entidad_emisora_alerta', count(*) FILTER (WHERE entidad_emisora_alerta IS NULL), count(*) FROM cun25123.gc_estructuraalertapredio
) AS deuda
WHERE nulos > 0
ORDER BY tabla, columna;
