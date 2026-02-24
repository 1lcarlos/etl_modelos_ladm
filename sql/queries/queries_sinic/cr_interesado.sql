-- Query para extraer datos de gc_interesado para migrar a cr_interesado
-- Origen: {schema}.gc_interesado
-- Destino: sinic2.cr_interesado
-- Fecha: 2026-02-02

SELECT DISTINCT ON (i.id)
    i.id,
    COALESCE(i.espacio_de_nombres, 'CR_INTERESADO') as espacio_de_nombres,
    COALESCE(i.local_id, i.id::varchar) as local_id,
    COALESCE(i.comienzo_vida_util_version, NOW()) as comienzo_vida_util_version,
    i.fin_vida_util_version,
    idt.text_code as tipo_documento,
    i.documento_identidad as numero_documento,
    i.primer_nombre,
    i.segundo_nombre,
    i.primer_apellido,
    i.segundo_apellido,
    st.text_code as sexo,
    get.text_code as grupo_etnico,
    NULL::boolean as autoreconocimientocampesino,
    i.razon_social,
    CASE
        WHEN i.razon_social IS NOT NULL AND i.razon_social != '' THEN i.razon_social
        ELSE TRIM(CONCAT_WS(' ',
            NULLIF(i.primer_nombre, ''),
            NULLIF(i.segundo_nombre, ''),
            NULLIF(i.primer_apellido, ''),
            NULLIF(i.segundo_apellido, '')
        ))
    END as nombre,
    it.text_code as tipo_interesado
FROM {schema}.gc_interesado i
LEFT JOIN {schema}.gc_interesadodocumentotipo idt ON i.tipo_documento = idt.id
LEFT JOIN {schema}.gc_sexotipo st ON i.sexo = st.id
LEFT JOIN {schema}.gc_grupoetnicotipo get ON i.grupo_etnico = get.id
LEFT JOIN {schema}.gc_interesadotipo it ON i.tipo = it.id
ORDER BY i.id;
