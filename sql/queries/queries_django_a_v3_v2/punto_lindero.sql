-- Destino: gc_puntolindero
WITH datos AS (
    SELECT
        pl.id,
        pl.id_punto,
        pl.geometria,
        pl.exactitud_horizontal,
        pl.exactitud_vertical,
        pl.comienzo_vida_util_version,
        pl.fin_vida_util_version,
        ac.text_code  AS acuerdo,
        fi.text_code  AS fotoidentificacion,
        mp.text_code  AS metodo_produccion,
        pt.text_code  AS punto_tipo
    FROM {schema}.gc_puntolindero AS pl
    LEFT JOIN {schema}.gc_acuerdotipo AS ac ON ac.id = pl.acuerdo
    LEFT JOIN {schema}.gc_fotoidentificaciontipo AS fi ON fi.id = pl.fotoidentificacion
    LEFT JOIN {schema}.col_metodoproducciontipo AS mp ON mp.id = pl.metodo_produccion
    LEFT JOIN {schema}.gc_puntotipo AS pt ON pt.id = pl.punto_tipo
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
