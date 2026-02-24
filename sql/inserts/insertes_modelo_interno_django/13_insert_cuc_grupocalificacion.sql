-- INSERT para tabla cuc_grupocalificacion (Modelo Interno Django)
-- Migra grupos de calificacion desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_calificacionconvencional (query cca_calificacionconvencional.sql)
-- Destino: cuc_grupocalificacion (modelo interno Django)
--
-- Normalizacion CCA -> Django:
--   - CCA tiene estructura PLANA: subtotal_estructura, subtotal_acabados, etc. en 1 fila
--   - Django normaliza: 1 calificacion -> 5 grupos (Estructura, Acabados, Banio, Cocina, Cerchas)
--   - Se usa UNION ALL para descomponer cada fila CCA en 5 filas de grupo
--
-- Generacion de IDs:
--   - id = cca_calificacion_id * 10 + grupo_offset
--   - Estructura: offset=1, Acabados: offset=2, Banio: offset=3, Cocina: offset=4, Cerchas: offset=5
--   - Ejemplo: calificacion_id=500 -> grupos: 5001, 5002, 5003, 5004, 5005
--
-- Dominios:
--   - clase_calificacion: FK a cuc_clasecalificaciontipo (text_code directo)
--   - conservacion: FK a cuc_estadoconservaciontipo (mapeo ilicode -> text_code -> id)
--
-- Dependencias:
--   - Requiere que cuc_calificacionconvencional ya este migrado (FASE 12)
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de cuc_calificacionconvencional

INSERT INTO {schema}.cuc_grupocalificacion (
    id,
    subtotal,
    clase_calificacion,
    conservacion,
    cuc_calificacion_convencional
)

-- ===== GRUPO 1: ESTRUCTURA =====
SELECT
    nextval('{schema}.cuc_grupocalificacion_id_seq'),
    --cc.cca_calificacion_id * 10 + 1,
    cc.subtotal_estructura::numeric,
    (SELECT id FROM {schema}.cuc_clasecalificaciontipo
     WHERE text_code = 'Estructura' LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_estadoconservaciontipo
         WHERE text_code = cc.conservacion_estructura LIMIT 1),
        (SELECT id FROM {schema}.cuc_estadoconservaciontipo
         WHERE text_code ILIKE '%' || cc.conservacion_estructura || '%' LIMIT 1),
        NULL
    ),
    (select id from {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric =  cc.cca_calificacion_id LIMIT 1)
    
FROM tmp_cca_calificacionconvencional cc
WHERE EXISTS (
    SELECT 1 FROM {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
)

UNION ALL

-- ===== GRUPO 2: ACABADOS PRINCIPALES =====
SELECT
    nextval('{schema}.cuc_grupocalificacion_id_seq'),
    --cc.cca_calificacion_id * 10 + 2,
    cc.subtotal_acabados::numeric,
    (SELECT id FROM {schema}.cuc_clasecalificaciontipo
     WHERE text_code = 'Acabados_Principales' LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_estadoconservaciontipo
         WHERE text_code = cc.conservacion_acabados LIMIT 1),
        (SELECT id FROM {schema}.cuc_estadoconservaciontipo
         WHERE text_code ILIKE '%' || cc.conservacion_acabados || '%' LIMIT 1),
        NULL
    ),
    (select id from {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric =  cc.cca_calificacion_id LIMIT 1)
FROM tmp_cca_calificacionconvencional cc
WHERE EXISTS (
    SELECT 1 FROM {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
)

UNION ALL

-- ===== GRUPO 3: BANIO =====
SELECT
    nextval('{schema}.cuc_grupocalificacion_id_seq'),
    --cc.cca_calificacion_id * 10 + 3,
    cc.subtotal_banio::numeric,
    (SELECT id FROM {schema}.cuc_clasecalificaciontipo
     WHERE text_code = 'Banio' LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_estadoconservaciontipo
         WHERE text_code = cc.conservacion_banio LIMIT 1),
        (SELECT id FROM {schema}.cuc_estadoconservaciontipo
         WHERE text_code ILIKE '%' || cc.conservacion_banio || '%' LIMIT 1),
        NULL
    ),
    (select id from {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric =  cc.cca_calificacion_id LIMIT 1)
FROM tmp_cca_calificacionconvencional cc
WHERE EXISTS (
    SELECT 1 FROM {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
)

UNION ALL

-- ===== GRUPO 4: COCINA =====
SELECT
    nextval('{schema}.cuc_grupocalificacion_id_seq'),
    --cc.cca_calificacion_id * 10 + 4,
    cc.subtotal_cocina::numeric,
    (SELECT id FROM {schema}.cuc_clasecalificaciontipo
     WHERE text_code = 'Cocina' LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_estadoconservaciontipo
         WHERE text_code = cc.conservacion_cocina LIMIT 1),
        (SELECT id FROM {schema}.cuc_estadoconservaciontipo
         WHERE text_code ILIKE '%' || cc.conservacion_cocina || '%' LIMIT 1),
        NULL
    ),
    (select id from {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric =  cc.cca_calificacion_id LIMIT 1)
FROM tmp_cca_calificacionconvencional cc
WHERE EXISTS (
    SELECT 1 FROM {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
)

UNION ALL

-- ===== GRUPO 5: COMPLEMENTO INDUSTRIA (CERCHAS) =====
SELECT
    nextval('{schema}.cuc_grupocalificacion_id_seq'),
    --cc.cca_calificacion_id * 10 + 5,
    cc.subtotal_cerchas::numeric,
    (SELECT id FROM {schema}.cuc_clasecalificaciontipo
     WHERE text_code = 'Complemento_Industria' LIMIT 1),
    NULL, -- Cerchas no tiene conservacion en CCA
    (select id from {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric =  cc.cca_calificacion_id LIMIT 1)
FROM tmp_cca_calificacionconvencional cc
WHERE EXISTS (
    SELECT 1 FROM {schema}.cuc_calificacionconvencional cal
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
);
