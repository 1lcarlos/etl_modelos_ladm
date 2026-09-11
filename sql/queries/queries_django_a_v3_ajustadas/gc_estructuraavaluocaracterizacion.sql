-- Avalúo catastral de caracterización de unidades de construcción
-- Fuente: extavaluo vinculado a gc_unidadconstruccion → gc_caracteristicasunidadconstruccion
SELECT
    ea.avaluo_catastral,
    ea.vigencia                 AS fecha_avaluo,
    NULL::numeric               AS valor_comercial,
    cuc.local_id                AS caracteristicas_local_id
FROM {schema}.extavaluo ea
JOIN {schema}.gc_unidadconstruccion uc
    ON uc.id = ea.gc_unidadconstruccion_avaluo
JOIN {schema}.gc_caracteristicasunidadconstruccion cuc
    ON cuc.id = uc.gc_caracteristicasunidadconstruccion
WHERE ea.gc_unidadconstruccion_avaluo IS NOT NULL
  AND ea.avaluo_catastral IS NOT NULL;
