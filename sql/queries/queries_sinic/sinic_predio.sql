-- Query para extraer datos de gc_predio para migrar a sinic_predio
-- Origen: {schema}.gc_predio
-- Destino: sinic2.sinic_predio
-- Fecha: 2026-02-02

SELECT DISTINCT ON (p.numero_predial)
    p.id,
    COALESCE(p.espacio_de_nombres, 'SINIC_PREDIO') as espacio_de_nombres,
    COALESCE(p.local_id, p.id::varchar) as local_id,
    COALESCE(p.comienzo_vida_util_version, NOW()) as comienzo_vida_util_version,
    p.fin_vida_util_version,
    p.departamento,
    p.municipio,
    p.codigo_orip,
    CASE
        WHEN p.matricula_inmobiliaria ilike '0' THEN NULL  
        WHEN p.matricula_inmobiliaria ilike '' THEN NULL
        WHEN p.matricula_inmobiliaria IS NULL THEN NULL
        WHEN length(p.matricula_inmobiliaria) >= 6 THEN 1  
        WHEN p.matricula_inmobiliaria ~ '^[0-9]+$' THEN p.matricula_inmobiliaria::numeric
        ELSE NULL
    END as matricula_inmobiliaria,
    p.numero_predial as numero_predial_nacional,
    COALESCE(
        NULLIF(p.nupre, ''),
        SUBSTRING(p.numero_predial FROM 1 FOR 11)
    ) as codigo_homologado,
    p.nupre,
    NULL::date as fecha_inscripcion_catastral,
    pt.text_code as tipo_predio,
    cpt.text_code as condicion_predio,
    det.text_code as destinacion_economica,
    COALESCE(p.area::numeric(25, 2), 0) as area_catastral_terreno,
    dalc.area_registral_m2 as area_registral_m2,
    CASE
        WHEN clst.text_code = 'Urbano' THEN ci.vigencia_urbana
        WHEN clst.text_code = 'Rural' THEN ci.vigencia_rural
        WHEN clst.text_code = 'Expansion_Urbana' THEN ci.vigencia_rural
        ELSE COALESCE(
            CASE
                WHEN SUBSTRING(p.numero_predial FROM 6 FOR 2) = '00'
                THEN (SELECT ci2.vigencia_rural FROM public.cadaster_information ci2 WHERE ci2.municipio = concat(p.departamento, p.municipio) LIMIT 1)
                ELSE (SELECT ci2.vigencia_urbana FROM public.cadaster_information ci2 WHERE ci2.municipio = concat(p.departamento, p.municipio) LIMIT 1)
            END, '1989-01-01'::date
        )
    END as vigencia_actualizacion_catastral,
    'Activo' as estado,
    CASE
        WHEN p.nombre IN ('', NULL) THEN 'Sin_direccion'
        ELSE p.nombre
    END as nombre,
    clst.text_code as clase_suelo,
    cst.text_code as categoria_suelo,
    p.numero_predial_anterior,
    av.avaluo_catastral
FROM {schema}.gc_predio p
LEFT JOIN {schema}.gc_prediotipo pt ON p.tipo = pt.id
LEFT JOIN {schema}.gc_categoriasuelotipo cst ON p.categoria_suelo = cst.id
LEFT JOIN {schema}.gc_clasesuelotipo clst ON p.clase_suelo = clst.id
LEFT JOIN {schema}.gc_condicionprediotipo cpt ON p.condicion_predio = cpt.id
LEFT JOIN {schema}.gc_destinacioneconomicatipo det ON p.destinacion_economica = det.id
LEFT JOIN {schema}.dlc_datosadicionaleslevantamientocatastral dalc ON p.id = dalc.gc_predio
LEFT JOIN public.cadaster_information ci ON ci.municipio = concat(p.departamento, p.municipio)
LEFT JOIN {schema}.extavaluo av ON p.id = av.gc_predio_avaluo
    AND av.vigencia = (SELECT MAX(vigencia) FROM {schema}.extavaluo av2 WHERE av2.gc_predio_avaluo = p.id)
ORDER BY p.numero_predial, p.comienzo_vida_util_version DESC;
