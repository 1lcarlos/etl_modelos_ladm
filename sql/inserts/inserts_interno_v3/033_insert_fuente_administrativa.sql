INSERT INTO
    {schema}.gc_fuenteadministrativa (
        t_id,
        t_ili_tid,
        tipo,
        ente_emisor,        
        estado_disponibilidad,        
        fecha_documento_fuente,
        nombre,
        descripcion,       
        espacio_de_nombres,
        local_id
    )
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    fat.t_id,    
    ente_emisor,
    edt.t_id,
    fecha_documento_fuente::date,
    numero_fuente,
    --concat(descripcion,' -Numero Fuente ', numero_fuente::text, ' - Valor Transaccion: ', valor_transaccion::text),
    descripcion,
    espacio_de_nombres,
    tmpfa.id    
FROM
    tmp_fuente_administrativa tmpfa
 JOIN {schema}.col_estadodisponibilidadtipo edt ON edt.ilicode ILIKE '%' ||  tmpfa.estado_disponibilidad || '%'
 JOIN {schema}.col_fuenteadministrativatipo fat ON fat.ilicode  ILIKE '%' ||  tmpfa.tipo_fuente_administrativa
 || '%'  
WHERE fat.baseclass is not null
