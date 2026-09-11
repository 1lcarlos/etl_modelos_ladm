-- Destino: gc_interesado
WITH datos AS (
    SELECT
        i.id,
        i.documento_identidad,
        i.primer_nombre,
        i.segundo_nombre,
        i.primer_apellido,
        i.segundo_apellido,
        i.razon_social,
        i.nombre,
        i.comienzo_vida_util_version,
        i.fin_vida_util_version,
        it.text_code  AS tipo_interesado,
        dt.text_code  AS tipo_documento,
        st.text_code  AS sexo,
        ge.text_code  AS grupo_etnico,
        ec.text_code  AS estado_civil,
        vt.text_code  AS verificado
    FROM {schema}.gc_interesado AS i
    LEFT JOIN {schema}.gc_interesadotipo AS it ON it.id = i.tipo
    LEFT JOIN {schema}.gc_interesadodocumentotipo AS dt ON dt.id = i.tipo_documento
    LEFT JOIN {schema}.gc_sexotipo AS st ON st.id = i.sexo
    LEFT JOIN {schema}.gc_grupoetnicotipo AS ge ON ge.id = i.grupo_etnico
    LEFT JOIN {schema}.gc_estadociviltipo AS ec ON ec.id = i.estado_civil
    LEFT JOIN {schema}.gc_verificaciontipo AS vt ON vt.id = i.verificado
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
