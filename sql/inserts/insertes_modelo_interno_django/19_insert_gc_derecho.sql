-- INSERT para tabla gc_derecho (Modelo Interno Django)
-- Migra derechos desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_derecho (query cca_derecho.sql)
-- Destino: gc_derecho (modelo interno Django)
--
-- Diferencias clave con modelo CCA:
--   - PK es 'id' (se usa cca_derecho_id)
--   - Dominios usan text_code (no ilicode) y FK apunta a 'id' (no t_id)
--   - CCA fraccion_derecho rango 0-100, Django rango 0-1 (dividir entre 100)
--   - CCA observacion -> Django descripcion
--   - CCA predio -> Django baunit
--   - CCA interesado -> Django interesado_gc_interesado
--   - CCA agrupacion_interesados -> Django interesado_gc_agrupacioninteresados
--   - CCA tiene cuota_participacion y origen_derecho que no existen en Django
--
-- Dependencias:
--   - Requiere que gc_predio ya este migrado (FASE 1)
--   - Requiere que gc_interesado ya este migrado (FASE 16)
--   - Requiere que gc_agrupacioninteresados ya este migrado (FASE 17)
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio, gc_interesado y gc_agrupacioninteresados

INSERT INTO {schema}.gc_derecho (
    id,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    descripcion,
    fraccion_derecho,
    fecha_inicio_tenencia,
    baunit,
    interesado_gc_interesado,
    interesado_gc_agrupacioninteresados,
    tipo
)
SELECT
    -- id: Usar cca_derecho_id como id en Django
    d.cca_derecho_id,

    -- espacio_de_nombres
    'GC_DERECHO_CCA',

    -- local_id
    d.cca_derecho_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- descripcion: CCA observacion -> Django descripcion
    d.observacion,

    -- fraccion_derecho: CCA rango 0-100 -> Django rango 0-1
    CASE
        WHEN d.fraccion_derecho IS NOT NULL
        THEN (d.fraccion_derecho::numeric / 100.0)::numeric
        ELSE NULL
    END,

    -- fecha_inicio_tenencia
    d.fecha_inicio_tenencia::date,

    -- baunit: FK a gc_predio por cca_predio_id
    (SELECT p.id FROM {schema}.gc_predio p
     WHERE p.id = d.cca_predio_id
     LIMIT 1),

    -- interesado_gc_interesado: FK a gc_interesado por cca_interesado_id
    (SELECT gi.id FROM {schema}.gc_interesado gi
     WHERE gi.id = d.cca_interesado_id::numeric
     LIMIT 1),

    -- interesado_gc_agrupacioninteresados: FK a gc_agrupacioninteresados por cca_agrupacion_interesados_id
    (SELECT ga.id FROM {schema}.gc_agrupacioninteresados ga
     WHERE ga.id = d.cca_agrupacion_interesados_id::numeric
     LIMIT 1),

    -- tipo: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_derechotipo
         WHERE text_code = d.tipo LIMIT 1),
        (SELECT id FROM {schema}.gc_derechotipo
         WHERE text_code ILIKE '%' || d.tipo || '%' LIMIT 1),
        NULL
    )

FROM tmp_cca_derecho d
WHERE EXISTS (
    SELECT 1 FROM {schema}.gc_predio p
    WHERE p.id = d.cca_predio_id
);
