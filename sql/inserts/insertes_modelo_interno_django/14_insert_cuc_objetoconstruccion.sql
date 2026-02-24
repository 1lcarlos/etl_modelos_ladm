-- INSERT para tabla cuc_objetoconstruccion (Modelo Interno Django)
-- Migra objetos de construccion desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_calificacionconvencional (query cca_calificacionconvencional.sql)
-- Destino: cuc_objetoconstruccion (modelo interno Django)
--
-- Normalizacion CCA -> Django:
--   - CCA tiene estructura PLANA: armazon, muros, cubierta, etc. como columnas FK
--   - Django normaliza: 1 grupo -> N objetos (cada componente es una fila)
--   - Se usa UNION ALL para descomponer cada grupo en sus objetos componentes
--
-- Generacion de IDs:
--   - id = nextval('{schema}.cuc_objetoconstruccion_id_seq')
--
-- Mapeo tipo_objeto_construccion:
--   - Django usa text_code con patron: 'Prefijo.' + ilicode
--   - Ejemplo: 'Armazon.' + 'Madera' -> 'Armazon.Madera' -> id=0
--   - Enchape y Mobiliario CCA comparten tabla pero Django usa prefijos separados
--     (Enchape_Banio.* vs Enchape_Cocina.*, Mobiliario_Banio.* vs Mobiliario_Cocina.*)
--   - Se usa ILIKE como fallback por posibles diferencias de mayusculas/minusculas
--
-- FK cuc_grupo_calificacion:
--   - Se busca via JOIN con cuc_calificacionconvencional (por local_id::numeric)
--     y filtrando por clase_calificacion (text_code del grupo)
--
-- Dependencias:
--   - Requiere que cuc_grupocalificacion ya este migrado (FASE 13)
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de cuc_grupocalificacion

INSERT INTO {schema}.cuc_objetoconstruccion (
    id,
    --puntos,
    cuc_grupo_calificacion,
    tipo_objeto_construccion
)

-- =============================================================================
-- GRUPO ESTRUCTURA: armazon, muros, cubierta
-- =============================================================================

-- Armazon
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Estructura' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Armazon.' || cc.armazon LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Armazon.' || cc.armazon LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.armazon IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Estructura' LIMIT 1)
)

UNION ALL

-- Muros
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Estructura' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Muros.' || cc.muros LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Muros.' || cc.muros LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.muros IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Estructura' LIMIT 1)
)

UNION ALL

-- Cubierta
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Estructura' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Cubierta.' || cc.cubierta LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Cubierta.' || cc.cubierta LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.cubierta IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Estructura' LIMIT 1)
)

UNION ALL

-- =============================================================================
-- GRUPO ACABADOS PRINCIPALES: fachada, cubrimiento_muros, piso
-- =============================================================================

-- Fachada
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Acabados_Principales' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Fachada.' || cc.fachada LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Fachada.' || cc.fachada LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.fachada IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Acabados_Principales' LIMIT 1)
)

UNION ALL

-- Cubrimiento Muros
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Acabados_Principales' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Cubrimiento_Muros.' || cc.cubrimiento_muros LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Cubrimiento_Muros.' || cc.cubrimiento_muros LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.cubrimiento_muros IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Acabados_Principales' LIMIT 1)
)

UNION ALL

-- Piso
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Acabados_Principales' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Piso.' || cc.piso LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Piso.' || cc.piso LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.piso IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Acabados_Principales' LIMIT 1)
)

UNION ALL

-- =============================================================================
-- GRUPO BANIO: tamanio_banio, enchape_banio, mobiliario_banio
-- =============================================================================

-- Tamanio Banio
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Banio' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Tamanio_Banio.' || cc.tamanio_banio LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Tamanio_Banio.' || cc.tamanio_banio LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.tamanio_banio IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Banio' LIMIT 1)
)

UNION ALL

-- Enchape Banio
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Banio' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Enchape_Banio.' || cc.enchape_banio LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Enchape_Banio.' || cc.enchape_banio LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.enchape_banio IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Banio' LIMIT 1)
)

UNION ALL

-- Mobiliario Banio
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Banio' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Mobiliario_Banio.' || cc.mobiliario_banio LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Mobiliario_Banio.' || cc.mobiliario_banio LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.mobiliario_banio IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Banio' LIMIT 1)
)

UNION ALL

-- =============================================================================
-- GRUPO COCINA: tamanio_cocina, enchape_cocina, mobiliario_cocina
-- =============================================================================

-- Tamanio Cocina
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Cocina' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Tamanio_Cocina.' || cc.tamanio_cocina LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Tamanio_Cocina.' || cc.tamanio_cocina LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.tamanio_cocina IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Cocina' LIMIT 1)
)

UNION ALL

-- Enchape Cocina
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Cocina' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Enchape_Cocina.' || cc.enchape_cocina LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Enchape_Cocina.' || cc.enchape_cocina LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.enchape_cocina IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Cocina' LIMIT 1)
)

UNION ALL

-- Mobiliario Cocina
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Cocina' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Mobiliario_Cocina.' || cc.mobiliario_cocina LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Mobiliario_Cocina.' || cc.mobiliario_cocina LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.mobiliario_cocina IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Cocina' LIMIT 1)
)

UNION ALL

-- =============================================================================
-- GRUPO CERCHAS / COMPLEMENTO INDUSTRIA: cerchas
-- =============================================================================

-- Cerchas
SELECT
    nextval('{schema}.cuc_objetoconstruccion_id_seq'),
    --NULL,
    (SELECT g.id FROM {schema}.cuc_grupocalificacion g
     INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
     WHERE cal.local_id::numeric = cc.cca_calificacion_id
     AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Complemento_Industria' LIMIT 1)
     LIMIT 1),
    COALESCE(
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code = 'Cerchas_Complemento_Industria.' || cc.cerchas LIMIT 1),
        (SELECT id FROM {schema}.cuc_objetoconstrucciontipo
         WHERE text_code ILIKE 'Cerchas_Complemento_Industria.' || cc.cerchas LIMIT 1),
        NULL
    )
FROM tmp_cca_calificacionconvencional cc
WHERE cc.cerchas IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.cuc_grupocalificacion g
    INNER JOIN {schema}.cuc_calificacionconvencional cal ON g.cuc_calificacion_convencional = cal.id
    WHERE cal.local_id::numeric = cc.cca_calificacion_id
    AND g.clase_calificacion = (SELECT id FROM {schema}.cuc_clasecalificaciontipo WHERE text_code = 'Complemento_Industria' LIMIT 1)
);
