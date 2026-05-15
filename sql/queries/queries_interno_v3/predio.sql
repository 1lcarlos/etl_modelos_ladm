-- Consulta para extraer datos de gc_predio del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los ilicode de las tablas de dominio para mapear a text_code en Django

SELECT DISTINCT
    COALESCE(p.espacio_de_nombres, 'gc_predio') as espacio_de_nombres,
    p.local_id,
    COALESCE(p.comienzo_vida_util_version::timestamp with time zone, now()) as comienzo_vida_util_version,
    p.fin_vida_util_version::timestamp with time zone as fin_vida_util_version,
    p.t_id,
    p.departamento,
    p.municipio,
    p.codigo_orip,
    CASE
        WHEN p.matricula_inmobiliaria IS NULL THEN '1'
        WHEN p.matricula_inmobiliaria <= 0 THEN '1'
        ELSE p.matricula_inmobiliaria::varchar
    END as matricula_inmobiliaria,
    p.numero_predial_nacional as numero_predial,
    p.numero_predial_anterior,
    p.nupre,
    COALESCE(p.area_catastral_terreno, 0) as area,
    p.area_construida,
    p.nombre,
    pt.ilicode as tipo_predio,
    cst.ilicode as categoria_suelo,
    clst.ilicode as clase_suelo,
    cpt.ilicode as condicion_predio,
    det.ilicode as destinacion_economica
FROM {schema}.gc_predio p
LEFT JOIN {schema}.gc_prediotipo pt ON p.tipo_predio = pt.t_id
LEFT JOIN {schema}.gc_categoriasuelotipo cst ON p.categoria_suelo = cst.t_id
LEFT JOIN {schema}.gc_clasesuelotipo clst ON p.clase_suelo = clst.t_id
LEFT JOIN {schema}.gc_condicionprediotipo cpt ON p.condicion_predio = cpt.t_id
LEFT JOIN {schema}.gc_destinacioneconomicatipo det ON p.destinacion_economica = det.t_id;
