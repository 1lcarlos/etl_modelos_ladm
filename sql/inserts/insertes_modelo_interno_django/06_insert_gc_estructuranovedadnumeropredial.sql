-- INSERT para tabla gc_estructuranovedadnumeropredial (Modelo Interno Django)
-- Migra estructura de novedades de numero predial desde modelo CCA
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_estructuranovedadnumeropredial (query cca_estructuranovedadnumeropredial.sql)
-- Destino: gc_estructuranovedadnumeropredial (modelo interno Django)
--
-- NOTA: Esta tabla NO se encontro en el backup parcial de destino (backup_parcial_gc_predio.sql).
--   Se asume que existe en el esquema completo con la siguiente estructura adaptada a Django:
--   - id (bigint, PK auto-generado)
--   - seq (bigint)
--   - numero_predial (varchar)
--   - tipo_novedad (bigint FK o varchar) -> mapeo ilicode a text_code
--   - dlc_datosadicionaleslevantamientocatastral (bigint FK)
--
--   Si la tabla tiene una estructura diferente, ajustar columnas segun corresponda.
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' auto-generado (no t_id)
--   - No existe t_seq (usa seq)
--   - Dominios usan text_code (no ilicode)
--   - FK referencia a dlc_datosadicionaleslevantamientocatastral (no gc_datosadicionaleslevantamientocatastral)
--
-- Dependencias:
--   - Requiere que gc_predio ya este migrado
--   - Requiere que dlc_datosadicionaleslevantamientocatastral ya este migrado
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de dlc_datosadicionaleslevantamientocatastral

INSERT INTO {schema}.gc_estructuranovedadnumeropredial (
    --seq,
    numero_predial,
    tipo_novedad,
    gc_predio_novedad_numeros_prediales
)
SELECT
    -- seq
    --COALESCE(e.t_seq::bigint, 0),

    -- numero_predial
    e.numero_predial,

    -- tipo_novedad: Mapeo ilicode (CCA) -> text_code (Django) -> id
    -- CCA tiene: Predio_Nuevo, Desenglobe, Englobe, Cancelacion, Cambio_Numero_Predial
    -- Django tiene valores mas detallados, usamos coincidencia escalonada
    COALESCE(
        -- Intento 1: Coincidencia exacta por text_code
        (SELECT id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
         WHERE ilicode = e.tipo_novedad LIMIT 1),
        -- Intento 2: Coincidencia parcial (valor CCA como prefijo)
        (SELECT id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
         WHERE ilicode ILIKE e.tipo_novedad || '.%' LIMIT 1),
        -- Intento 3: Coincidencia inversa (text_code contiene el valor CCA)
        (SELECT id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
         WHERE ilicode ILIKE '%' || e.tipo_novedad || '%' LIMIT 1),
        -- Intento 4: Mapeo especifico para casos conocidos
        CASE
            WHEN e.tipo_novedad = 'Predio_Nuevo' THEN
                (SELECT id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode = 'Predio_Nuevo' LIMIT 1)
            WHEN e.tipo_novedad = 'Desenglobe' THEN
                (SELECT id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode = 'Desenglobe' LIMIT 1)
            WHEN e.tipo_novedad = 'Englobe' THEN
                (SELECT id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode = 'Englobe' LIMIT 1)
            WHEN e.tipo_novedad = 'Cancelacion' THEN
                (SELECT id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode ILIKE 'Cancelacion.%' LIMIT 1)
            WHEN e.tipo_novedad = 'Cambio_Numero_Predial' THEN
                (SELECT id FROM {schema}.gc_estructuranovedadnumeropredial_tipo_novedad
                 WHERE ilicode ILIKE 'Cambio_Numero_Predial.%' LIMIT 1)
            ELSE NULL
        END
    ),

    -- dlc_dtsdcnlslvntmntctstral_novedad_numeros_prediales:
    -- Buscar el id de dlc_datosadicionaleslevantamientocatastral usando el numero_predial del predio
    (SELECT dalc.id
     FROM {schema}.dlc_datosadicionaleslevantamientocatastral dalc
     INNER JOIN {schema}.gc_predio gp ON dalc.gc_predio = gp.id
     WHERE gp.numero_predial = e.numero_predial_predio
     LIMIT 1)

FROM tmp_cca_estructuranovedadnumeropredial e
WHERE EXISTS (
    -- Solo insertar si existe el registro en dlc_datosadicionaleslevantamientocatastral
    SELECT 1
    FROM {schema}.dlc_datosadicionaleslevantamientocatastral dalc
    INNER JOIN {schema}.gc_predio gp ON dalc.gc_predio = gp.id
    WHERE gp.numero_predial = e.numero_predial_predio
);
