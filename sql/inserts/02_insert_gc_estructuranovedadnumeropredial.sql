-- Insert para gc_estructuranovedadnumeropredial
-- Origen: tmp_cca_estructuranovedadnumeropredial (datos de cca_estructuranovedadnumeropredial)
-- Destino: gc_estructuranovedadnumeropredial
-- Versión: 1.0
-- Fecha: 2026-02-04
--
-- Notas:
-- 1. Este insert debe ejecutarse DESPUES de gc_datosadicionaleslevantamientocatastral
-- 2. El mapeo de tipo_novedad es de valores simples (CCA) a valores compuestos (GC)
--    Ejemplo: "Predio_Nuevo" -> "Predio_Nuevo.Predio_Nuevo_Formal"
-- 3. La relación es con gc_datosadicionaleslevantamientocatastral, no directamente con gc_predio

INSERT INTO {schema}.gc_estructuranovedadnumeropredial (
    t_id,
    t_seq,
    numero_predial,
    tipo_novedad,
    gc_dtsdcnlstmntctstral_novedad_numeros_prediales
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    e.t_seq,
    e.numero_predial,

    -- tipo_novedad: Mapeo de valores CCA a valores GC
    -- CCA tiene: Predio_Nuevo, Desenglobe, Englobe, Cancelacion, Cambio_Numero_Predial
    -- GC tiene valores más detallados, usamos el primero que coincida
    COALESCE(
        -- Intento 1: Coincidencia exacta
        (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
         WHERE ilicode = e.tipo_novedad LIMIT 1),
        -- Intento 2: Coincidencia parcial (el valor CCA está contenido en ilicode de GC)
        (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
         WHERE ilicode ILIKE e.tipo_novedad || '.%' LIMIT 1),
        -- Intento 3: Coincidencia inversa (el ilicode de GC contiene el valor CCA)
        (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
         WHERE ilicode ILIKE '%' || e.tipo_novedad || '%' LIMIT 1),
        -- Intento 4: Mapeo específico para casos conocidos
        CASE
            WHEN e.tipo_novedad = 'Predio_Nuevo' THEN
                (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode = 'Predio_Nuevo_Formal' LIMIT 1)
            WHEN e.tipo_novedad = 'Desenglobe' THEN
                (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode = 'Desenglobe' LIMIT 1)
            WHEN e.tipo_novedad = 'Englobe' THEN
                (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode = 'Englobe' LIMIT 1)
            WHEN e.tipo_novedad = 'Cancelacion' THEN
                (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode ILIKE 'Cancelacion.%' LIMIT 1)
            WHEN e.tipo_novedad = 'Cambio_Numero_Predial' THEN
                (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode ILIKE 'Cambio_Numero_Predial.%' LIMIT 1)
            ELSE
                (SELECT t_id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode = 'Ninguna' LIMIT 1)
        END
    ) AS tipo_novedad,

    -- gc_dtsdcnlstmntctstral_novedad_numeros_prediales:
    -- Buscar el t_id de gc_datosadicionaleslevantamientocatastral usando el numero_predial del predio
    (SELECT dalc.t_id
     FROM {schema}.gc_datosadicionaleslevantamientocatastral dalc
     INNER JOIN {schema}.gc_predio gp ON dalc.gc_predio = gp.t_id
     WHERE gp.numero_predial_nacional = e.numero_predial_predio
     LIMIT 1) AS gc_dtsdcnlstmntctstral_novedad_numeros_prediales

FROM tmp_cca_estructuranovedadnumeropredial e
WHERE EXISTS (
    -- Solo insertar si existe el registro en gc_datosadicionaleslevantamientocatastral
    SELECT 1
    FROM {schema}.gc_datosadicionaleslevantamientocatastral dalc
    INNER JOIN {schema}.gc_predio gp ON dalc.gc_predio = gp.t_id
    WHERE gp.numero_predial_nacional = e.numero_predial_predio
);
