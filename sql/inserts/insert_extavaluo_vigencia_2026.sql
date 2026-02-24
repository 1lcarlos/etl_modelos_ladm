-- =============================================================================
-- Script: Insertar registros de vigencia 2026 en extavaluo
-- Descripcion: Toma los registros de vigencia 2025-01-01, aplica un incremento
--              del 3% al avaluo_catastral e inserta nuevos registros para la
--              vigencia 2026-01-01.
-- Schema: cun25436
-- Fecha: 2026-02-11
-- =============================================================================

-- =============================================
-- 1. CONSULTA DE VERIFICACION PREVIA
--    Ejecutar primero para validar cuantos registros se van a insertar
-- =============================================

-- Contar registros de vigencia 2025 que NO tienen vigencia 2026
SELECT
    COUNT(*) AS registros_a_insertar
FROM cun25436.extavaluo e25
WHERE e25.vigencia = '2025-01-01'
AND NOT EXISTS (
    SELECT 1
    FROM cun25436.extavaluo e26
    WHERE e26.vigencia = '2026-01-01'
    AND COALESCE(e26.gc_predio_avaluo, -1) = COALESCE(e25.gc_predio_avaluo, -1)
    AND COALESCE(e26.gc_terreno_avaluo, -1) = COALESCE(e25.gc_terreno_avaluo, -1)
    AND COALESCE(e26.gc_construccion_avaluo, -1) = COALESCE(e25.gc_construccion_avaluo, -1)
    AND COALESCE(e26.gc_servidumbretransito_avaluo, -1) = COALESCE(e25.gc_servidumbretransito_avaluo, -1)
    AND COALESCE(e26.gc_unidadconstruccion_avaluo, -1) = COALESCE(e25.gc_unidadconstruccion_avaluo, -1)
);

-- Vista previa de los primeros 20 registros que se van a insertar
SELECT
    e25.id AS id_origen_2025,
    e25.avaluo_catastral AS avaluo_2025,
    ROUND(e25.avaluo_catastral * 1.03, -3) AS avaluo_2026_incrementado,
    e25.gc_predio_avaluo,
    e25.gc_terreno_avaluo,
    e25.gc_construccion_avaluo,
    e25.gc_servidumbretransito_avaluo,
    e25.gc_unidadconstruccion_avaluo
FROM cun25436.extavaluo e25
WHERE e25.vigencia = '2025-01-01'
AND NOT EXISTS (
    SELECT 1
    FROM cun25436.extavaluo e26
    WHERE e26.vigencia = '2026-01-01'
    AND COALESCE(e26.gc_predio_avaluo, -1) = COALESCE(e25.gc_predio_avaluo, -1)
    AND COALESCE(e26.gc_terreno_avaluo, -1) = COALESCE(e25.gc_terreno_avaluo, -1)
    AND COALESCE(e26.gc_construccion_avaluo, -1) = COALESCE(e25.gc_construccion_avaluo, -1)
    AND COALESCE(e26.gc_servidumbretransito_avaluo, -1) = COALESCE(e25.gc_servidumbretransito_avaluo, -1)
    AND COALESCE(e26.gc_unidadconstruccion_avaluo, -1) = COALESCE(e25.gc_unidadconstruccion_avaluo, -1)
)
ORDER BY e25.id
LIMIT 20;

-- =============================================
-- 2. INSERT DE VIGENCIA 2026
--    Incremento del 3% sobre avaluo_catastral de vigencia 2025
-- =============================================

BEGIN;

INSERT INTO cun25436.extavaluo (
    --seq,
    avaluo_catastral,
    vigencia,
    por_decreto,
    --descripcion,
    --gc_construccion_avaluo,
    gc_predio_avaluo,
    --gc_servidumbretransito_avaluo,
    --gc_terreno_avaluo,
    --gc_unidadconstruccion_avaluo,
    modificado
)
SELECT
    --e25.seq,
    ROUND(e25.avaluo_catastral * 1.03, -3) AS avaluo_catastral,
    '2026-01-01'::date AS vigencia,
    TRUE AS por_decreto,
    --'Automático fin 2025' AS descripcion,
    --e25.gc_construccion_avaluo,
    e25.gc_predio_avaluo,
    --e25.gc_servidumbretransito_avaluo,
    --e25.gc_terreno_avaluo,
    --e25.gc_unidadconstruccion_avaluo,
    FALSE AS modificado
FROM cun25436.extavaluo e25
WHERE e25.vigencia = '2025-01-01'
AND NOT EXISTS (
    SELECT 1
    FROM cun25436.extavaluo e26
    WHERE e26.vigencia = '2026-01-01'
    AND COALESCE(e26.gc_predio_avaluo, -1) = COALESCE(e25.gc_predio_avaluo, -1)
    AND COALESCE(e26.gc_terreno_avaluo, -1) = COALESCE(e25.gc_terreno_avaluo, -1)
    AND COALESCE(e26.gc_construccion_avaluo, -1) = COALESCE(e25.gc_construccion_avaluo, -1)
    AND COALESCE(e26.gc_servidumbretransito_avaluo, -1) = COALESCE(e25.gc_servidumbretransito_avaluo, -1)
    AND COALESCE(e26.gc_unidadconstruccion_avaluo, -1) = COALESCE(e25.gc_unidadconstruccion_avaluo, -1)
)
ORDER BY e25.id;

-- =============================================
-- 3. VERIFICACION POSTERIOR
-- =============================================

-- Contar registros insertados para vigencia 2026
SELECT
    COUNT(*) AS total_registros_2026
FROM cun25436.extavaluo
WHERE vigencia = '2026-01-01';

-- Comparativo de totales por vigencia
SELECT
    vigencia,
    COUNT(*) AS total_registros,
    ROUND(AVG(avaluo_catastral), 1) AS promedio_avaluo,
    ROUND(MIN(avaluo_catastral), 1) AS min_avaluo,
    ROUND(MAX(avaluo_catastral), 1) AS max_avaluo
FROM cun25436.extavaluo
WHERE vigencia IN ('2025-01-01', '2026-01-01')
GROUP BY vigencia
ORDER BY vigencia;

-- Muestra de 10 registros comparando 2025 vs 2026
SELECT
    e25.gc_predio_avaluo,
    e25.avaluo_catastral AS avaluo_2025,
    e26.avaluo_catastral AS avaluo_2026,
    ROUND(((e26.avaluo_catastral - e25.avaluo_catastral) / e25.avaluo_catastral) * 100, 2) AS pct_incremento
FROM cun25436.extavaluo e25
INNER JOIN cun25436.extavaluo e26
    ON e26.gc_predio_avaluo = e25.gc_predio_avaluo
WHERE e25.vigencia = '2025-01-01'
AND e26.vigencia = '2026-01-01'
ORDER BY e25.gc_predio_avaluo
LIMIT 10;

-- Si todo esta correcto, ejecutar:
COMMIT;

-- Si algo fallo o los datos no son correctos, ejecutar en su lugar:
-- ROLLBACK;

-- =============================================
-- 4. ACTUALIZAR SECUENCIA (ejecutar despues del commit)
-- =============================================
SELECT setval(
    'cun25436.extavaluo_id_seq',
    (SELECT MAX(id) FROM cun25436.extavaluo)
);
