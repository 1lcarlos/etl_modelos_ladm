INSERT INTO
    {schema}.gc_datosadicionaleslevantamientocatastral (
        t_id,
        t_ili_tid,
        observaciones,
        fecha_visita_predial,
        tipo_documento_reconocedor,
        numero_documento_reconocedor,
        primer_nombre_reconocedor,
        segundo_nombre_reconocedor,
        primer_apellido_reconocedor,
        segundo_apellido_reconocedor,
        resultado_visita,
        otro_cual_resultado_visita,
        suscribe_acta_colindancia,
        despojo_abandono,
        estrato,
        otro_cual_estrato,       
        gc_predio
    )
select
        nextval('{schema}.t_ili2db_seq'::regclass),
        uuid_generate_v4 (),
        tdalc.observaciones,
        tdalc.fecha_visita_predial::date,
        --dt.t_id,
        COALESCE(
            (SELECT t_id FROM {schema}.col_documentotipo
            WHERE ilicode = tdalc.tipo_documento_reconocedor and baseclass is not null LIMIT 1),
            (SELECT t_id FROM {schema}.col_documentotipo
            WHERE ilicode ILIKE '%' || tdalc.tipo_documento_reconocedor || '%' and baseclass is not null LIMIT 1),
            (SELECT t_id FROM {schema}.col_documentotipo
            WHERE ilicode = 'Sin_Informacion' and baseclass is not null  LIMIT 1)
        ) AS tipo_documento_reconocedor,
        tdalc.numero_documento_reconocedor,
        tdalc.primer_nombre_reconocedor,
        tdalc.segundo_nombre_reconocedor,
        tdalc.primer_apellido_reconocedor,
        tdalc.segundo_apellido_reconocedor,
        --rvt.t_id,
         COALESCE(
            (SELECT t_id FROM {schema}.gc_resultadovisitatipo
            WHERE ilicode = tdalc.resultado_visita LIMIT 1),
            (SELECT t_id FROM {schema}.gc_resultadovisitatipo
            WHERE ilicode ILIKE '%' || tdalc.resultado_visita || '%' LIMIT 1),
            (SELECT t_id FROM {schema}.gc_resultadovisitatipo
            WHERE ilicode = 'Sin_Visita' LIMIT 1)
        ) AS resultado_visita,
        tdalc.otro_cual_resultado_visita,
        tdalc.suscribe_acta_colindancia::boolean,
        tdalc.despojo_abandono::boolean,
        tdalc.estrato::integer,
        tdalc.otro_cual_estrato,        
        p.t_id
    from tmp_datos_adicionales_lev_catastral tdalc
    --JOIN {schema}.col_documentotipo dt ON dt.ilicode = tdalc.tipo_documento_reconocedor
    --JOIN {schema}.gc_resultadovisitatipo rvt on rvt.ilicode = tdalc.resultado_visita
    JOIN {schema}.gc_predio p on p.local_id::integer = tdalc.gc_predio
    --WHERE dt.baseclass is not null;