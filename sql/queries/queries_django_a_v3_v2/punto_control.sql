-- Destino: gc_puntocontrol
WITH datos AS (
    SELECT
        pc.id,
        pc.id_punto,
        pc.geometria,
        pc.exactitud_horizontal,
        pc.exactitud_vertical,
        pc.comienzo_vida_util_version,
        pc.fin_vida_util_version,
        mp.text_code  AS metodo_produccion,
        pt.text_code  AS punto_tipo,
        tpc.text_code AS tipo_punto_control
    FROM {schema}.gc_puntocontrol AS pc
    LEFT JOIN {schema}.col_metodoproducciontipo AS mp ON mp.id = pc.metodo_produccion
    LEFT JOIN {schema}.gc_puntotipo AS pt ON pt.id = pc.punto_tipo
    LEFT JOIN {schema}.gc_puntocontroltipo AS tpc ON tpc.id = pc.tipo_punto_control
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
