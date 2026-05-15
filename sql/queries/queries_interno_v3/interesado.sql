-- Consulta para extraer datos de gc_interesado del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los ilicode de las tablas de dominio para mapear a text_code en Django

SELECT DISTINCT
    COALESCE(i.espacio_de_nombres, 'gc_interesado') as espacio_de_nombres,
    i.local_id,
    COALESCE(i.comienzo_vida_util_version::timestamp with time zone, now()) as comienzo_vida_util_version,
    i.fin_vida_util_version::timestamp with time zone as fin_vida_util_version,
    i.nombre,
    i.t_id as id_interesado,
    i.numero_documento as documento_identidad,
    i.primer_nombre,
    i.segundo_nombre,
    i.primer_apellido,
    i.segundo_apellido,
    i.razon_social,
    ect.ilicode as estado_civil,
    autrec.ilicode as grupo_etnico,
    st.ilicode as sexo,
    it.ilicode as interesadotipo,
    CASE
        WHEN dt.ilicode IS NULL THEN 'Sin_Informacion'
        ELSE dt.ilicode
    END as tipo_documento
FROM {schema}.gc_interesado i
LEFT JOIN {schema}.gc_estadociviltipo ect ON i.estado_civil = ect.t_id
LEFT JOIN {schema}.gc_autoreconocimientoetnicotipo autrec ON i.autoreconocimientoetnico = autrec.t_id
LEFT JOIN {schema}.gc_sexotipo st ON i.sexo = st.t_id
LEFT JOIN {schema}.col_interesadotipo it ON i.tipo_interesado = it.t_id
LEFT JOIN {schema}.col_documentotipo dt ON i.tipo_documento = dt.t_id;
