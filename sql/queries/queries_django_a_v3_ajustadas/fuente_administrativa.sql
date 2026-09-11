SELECT
    espacio_de_nombres,
    local_id,
    ente_emisor,
    fecha_documento_fuente,
    descripcion,
    numero_fuente,
    fa.id,
    valor_transaccion,
    coalesce(edt.text_code, 'Desconocido') as estado_disponibilidad,
    coalesce(fat.text_code, 'Sin_Documento') as tipo_fuente_administrativa
FROM
    {schema}.gc_fuenteadministrativa fa
left JOIN {schema}.col_estadodisponibilidadtipo edt ON edt.id = fa.estado_disponibilidad
left JOIN {schema}.gc_fuenteadministrativatipo fat ON fat.id = fa.tipo;