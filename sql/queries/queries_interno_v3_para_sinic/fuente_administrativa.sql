-- Consulta para extraer datos de gc_fuenteadministrativa del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los ilicode de las tablas de dominio para mapear a text_code en Django
-- v3.nombre se mapea de vuelta a Django.numero_fuente

SELECT
    distinct on (fa.t_id::bigint)
    COALESCE(fa.espacio_de_nombres, 'gc_fuenteadministrativa') as espacio_de_nombres,
    fa.local_id,
    fa.ente_emisor,
    fa.fecha_documento_fuente,
    fa.descripcion,
    fa.nombre as numero_fuente,
    fa.t_id,
    COALESCE(edt.ilicode, 'Desconocido') as estado_disponibilidad,
    COALESCE(fat.ilicode, 'Sin_Documento') as tipo_fuente_administrativa
FROM {schema}.gc_fuenteadministrativa fa
LEFT JOIN {schema}.col_estadodisponibilidadtipo edt ON edt.t_id = fa.estado_disponibilidad
LEFT JOIN {schema}.col_fuenteadministrativatipo fat ON fat.t_id = fa.tipo
join {schema}.col_unidadfuente uf ON fa.t_id = uf.fuente_administrativa
join {schema}.gc_predio p ON uf.unidad = p.t_id
JOIN {schema}.gc_predio_tramitecatastral ptc ON p.t_id = ptc.cr_predio;
