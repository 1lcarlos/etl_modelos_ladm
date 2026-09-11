INSERT INTO {schema}.cr_interesado
(t_id
, t_basket
, t_ili_tid
, tipo_documento
, primer_nombre
, segundo_nombre
, primer_apellido
, segundo_apellido
, sexo
, autoreconocimientoetnico
, razon_social
, nombre
, tipo_interesado
, numero_documento
, comienzo_vida_util_version
, espacio_de_nombres
, local_id)
SELECT DISTINCT on (i.id_interesado)
    i.id_interesado
    ,(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1)
    ,uuid_generate_v4()
    ,COALESCE(
        (SELECT t_id FROM {schema}.col_documentotipo WHERE ilicode = i.tipo_documento LIMIT 1),
        (SELECT t_id FROM {schema}.col_documentotipo WHERE ilicode = 'Cedula_Ciudadania' LIMIT 1)
    )
    ,i.primer_nombre
    ,i.segundo_nombre
    ,i.primer_apellido
    ,i.segundo_apellido
    -- sexo: Mapeo a cr_sexotipo (NOT NULL)
    ,COALESCE(
        (SELECT t_id FROM {schema}.cr_sexotipo WHERE ilicode = i.sexo LIMIT 1),
        (SELECT t_id FROM {schema}.cr_sexotipo WHERE ilicode = 'Sin_Determinar' LIMIT 1)
    )
    -- autoreconocimientoetnico: Mapeo a cr_autoreconocimientoetnicotipo
    ,(SELECT t_id FROM {schema}.cr_autoreconocimientoetnicotipo WHERE ilicode = i.grupo_etnico LIMIT 1)    
    ,i.razon_social
    ,i.nombre
    ,(SELECT t_id FROM {schema}.col_interesadotipo WHERE ilicode = i.interesadotipo LIMIT 1)
    ,i.documento_identidad
    ,COALESCE(i.comienzo_vida_util_version::timestamp with time zone, now())
    ,COALESCE(i.espacio_de_nombres, 'gc_interesado') as espacio_de_nombres
    ,i.local_id
FROM tmp_interesados_agrupacion i
where i.id_interesado not in (select t_id from {schema}.cr_interesado);