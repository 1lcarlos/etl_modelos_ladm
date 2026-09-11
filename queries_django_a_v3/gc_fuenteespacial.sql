SELECT
    fe.espacio_de_nombres,
    fe.local_id,
    fe.descripcion,
    fe.nombre,
    fe.fecha_documento_fuente,
    COALESCE(fet.text_code, 'Croquis_Campo')   AS tipo_fuente_espacial,
    COALESCE(edt.text_code, 'Desconocido')     AS estado_disponibilidad
FROM {schema}.gc_fuenteespacial fe
LEFT JOIN {schema}.gc_fuenteespacialtipo    fet ON fet.id = fe.tipo
LEFT JOIN {schema}.col_estadodisponibilidadtipo edt ON edt.id = fe.estado_disponibilidad;
