-- Consulta para extraer datos de cca_predio para migrar a gc_predio
-- Origen: Modelo CCA (cca_predio)
-- Destino: gc_predio
-- Fecha: 2026-02-05
--
-- Notas:
-- 1. departamento_municipio se divide en departamento (2 chars) y municipio (3 chars)
-- 2. tiene_fmi en CCA es bigint (dominio), en GC es boolean
-- 3. Los campos de dominio se mapean usando ilicode

SELECT
    distinct on (p.t_id, p.numero_predial)
    p.t_id as cca_predio_id,
    p.numero_predial,
    p.numero_predial_anterior,
    p.nupre,

    -- Separar departamento_municipio en departamento (2 chars) y municipio (3 chars)
    SUBSTRING(p.departamento_municipio, 1, 2) as departamento,
    SUBSTRING(p.departamento_municipio, 3, 3) as municipio,

    -- tiene_fmi: Convertir de dominio bigint a boolean
    CASE
        WHEN tfmi.ilicode = 'Si' THEN true
        WHEN tfmi.ilicode = 'No' THEN false
        ELSE NULL
    END as tiene_fmi,

    p.codigo_orip,
    p.matricula_inmobiliaria,

    -- Areas
    t.area_terreno,
    --p.area_registral_m2 as area_registral, 
    c.area_construccion_digital as area_construida,
    -- codigo nupre
    p.nupre as nupre,
    -- Dominios con ilicode para mapeo en destino
    cst.ilicode as categoria_suelo,
    csrt.ilicode as clase_suelo,
    cpt.ilicode as condicion_predio,
    det.ilicode as destinacion_economica
    --,ptt.ilicode as tipo_predio,
    ,SUBSTRING(ptt.ilicode FROM '[^.]+$') AS tipo_predio
    -- Campos adicionales que pueden ser utiles
    --p.codigo_homologado,
    ,p.id_operacion
    --direccion
    ,e.nombre_predio as nombre
FROM {schema}.cca_predio p
left join {schema}.extdireccion e on p.t_id = e.cca_predio_direccion 
left join {schema}.cca_terreno t on p.t_id = t.predio
left join {schema}.cca_construccion c on p.t_id = c.predio 
LEFT JOIN {schema}.cca_categoriasuelotipo cst ON p.categoria_suelo = cst.t_id
LEFT JOIN {schema}.cca_clasesuelotipo csrt ON p.clase_suelo_registro = csrt.t_id
LEFT JOIN {schema}.cca_condicionprediotipo cpt ON p.condicion_predio = cpt.t_id
LEFT JOIN {schema}.cca_destinacioneconomicatipo det ON p.destinacion_economica = det.t_id
LEFT JOIN {schema}.cca_prediotipo ptt ON p.predio_tipo = ptt.t_id
LEFT JOIN {schema}.cca_booleanotipo tfmi ON p.tiene_fmi = tfmi.t_id
LEFT JOIN {schema}.cca_derecho d ON p.t_id = d.predio
WHERE d.t_id IS NOT NULL;
