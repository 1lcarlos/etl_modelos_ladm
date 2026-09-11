-- cuc_calificacionconvencional no tiene FK directa a gc_caracteristicasunidadconstruccion
-- La tabla está vacía en todos los esquemas; se retorna NULL para gc_caracteristica_uconstruccion
SELECT
cc.total_calificacion,
ca.text_code as tipo_calificar,
NULL::bigint as gc_caracteristica_uconstruccion
FROM {schema}.cuc_calificacionconvencional cc
JOIN {schema}.cuc_calificartipo ca ON ca.id = cc.tipo_calificar;