-- Destino: gc_predio
-- Origen : gc_predio + dlc_datosadicionaleslevantamientocatastral (area registral)
--          + public.cadaster_information (vigencia de actualizacion catastral)
-- Fila centinela: ver README del flujo (es_centinela = true en la primera fila).
WITH vigencia AS (
    SELECT DISTINCT ON (ci.municipio::text)
        ci.municipio::text AS municipio,
        ci.vigencia_urbana,
        ci.vigencia_rural
    FROM public.cadaster_information AS ci
    ORDER BY ci.municipio::text
),
datos AS (
    SELECT
        p.id,
        p.departamento,
        p.municipio,
        p.codigo_orip,
        p.matricula_inmobiliaria,
        p.numero_predial,
        p.numero_predial_anterior,
        p.nupre,
        p.area,
        p.area_construida,
        p.nombre,
        p.comienzo_vida_util_version,
        p.fin_vida_util_version,
        pt.text_code   AS tipo_predio,
        cpt.text_code  AS condicion_predio,
        det.text_code  AS destinacion_economica,
        cst.text_code  AS categoria_suelo,
        clst.text_code AS clase_suelo,
        dlc.area_registral_m2,
        CASE
            WHEN clst.text_code = 'Urbano' THEN v.vigencia_urbana
            WHEN clst.text_code IN ('Rural', 'Expansion_Urbana') THEN v.vigencia_rural
        END AS vigencia_actualizacion_catastral
    FROM {schema}.gc_predio AS p
    LEFT JOIN {schema}.gc_prediotipo AS pt ON pt.id = p.tipo
    LEFT JOIN {schema}.gc_condicionprediotipo AS cpt ON cpt.id = p.condicion_predio
    LEFT JOIN {schema}.gc_destinacioneconomicatipo AS det ON det.id = p.destinacion_economica
    LEFT JOIN {schema}.gc_categoriasuelotipo AS cst ON cst.id = p.categoria_suelo
    LEFT JOIN {schema}.gc_clasesuelotipo AS clst ON clst.id = p.clase_suelo
    LEFT JOIN LATERAL (
        SELECT d.area_registral_m2
        FROM {schema}.dlc_datosadicionaleslevantamientocatastral AS d
        WHERE d.gc_predio = p.id
        ORDER BY d.id DESC
        LIMIT 1
    ) AS dlc ON true
    LEFT JOIN vigencia AS v ON v.municipio = p.departamento || p.municipio
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
