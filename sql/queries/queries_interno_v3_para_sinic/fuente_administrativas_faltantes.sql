SELECT
    distinct on (fa.t_id::bigint)
    COALESCE(fa.espacio_de_nombres, 'gc_fuenteadministrativa') as espacio_de_nombres,
    fa.local_id,
    fa.ente_emisor,
    fa.fecha_documento_fuente,
    fa.descripcion,
    fa.nombre as numero_fuente,
    fa.t_id as id_fuente_administrativa,
    COALESCE(edt.ilicode, 'Desconocido') as estado_disponibilidad,
    COALESCE(fat.ilicode, 'Sin_Documento') as tipo_fuente_administrativa
FROM {schema}.gc_derechocatastral dc
join {schema}.col_rrrfuente rrr on dc.t_id= rrr.rrr_gc_derechocatastral
join {schema}.gc_predio p ON dc.unidad = p.t_id 
join {schema}.gc_fuenteadministrativa fa on rrr.fuente_administrativa = fa.t_id
LEFT JOIN {schema}.col_estadodisponibilidadtipo edt ON edt.t_id = fa.estado_disponibilidad
LEFT JOIN {schema}.col_fuenteadministrativatipo fat ON fat.t_id = fa.tipo
JOIN {schema}.gc_predio_tramitecatastral ptc ON p.t_id = ptc.cr_predio;
