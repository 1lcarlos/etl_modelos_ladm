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
    LEFT(ente_emisor, 255),
    edt.t_id,
    fecha_documento_fuente::date,
    LEFT(numero_fuente::text, 255),
    LEFT(concat(descripcion,' -Numero Fuente ', numero_fuente::text, ' - Valor Transaccion: ', valor_transaccion::text), 255),
    LEFT(espacio_de_nombres, 255),
    tmpfa.id    
FROM
    tmp_fuente_administrativa tmpfa
 JOIN {schema}.col_estadodisponibilidadtipo edt ON edt.ilicode ILIKE '%' ||  tmpfa.estado_disponibilidad || '%'
 JOIN {schema}.col_fuenteadministrativatipo fat ON fat.ilicode  ILIKE '%' ||  tmpfa.tipo_fuente_administrativa
 || '%'  
WHERE fat.baseclass is not null
