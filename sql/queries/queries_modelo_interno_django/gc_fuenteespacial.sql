-- Query para extraer datos de gc_fuenteespacial
-- Fecha: 2025-12-18

SELECT
    fe.id,
    COALESCE(fe.espacio_de_nombres, 'GC_FUENTEESPACIAL') as espacio_de_nombres,
    COALESCE(fe.local_id, fe.id::varchar) as local_id,
    fe.nombre,
    COALESCE(fe.descripcion, 'Sin descripcion') as descripcion,
    fe.fecha_documento_fuente,
    COALESCE(edt.text_code, 'Disponible') as estado_disponibilidad,
    COALESCE(fet.text_code, 'Documento') as tipo_fuente
FROM {schema}.gc_fuenteespacial fe
LEFT JOIN {schema}.col_estadodisponibilidadtipo edt ON edt.id = fe.estado_disponibilidad
LEFT JOIN {schema}.gc_fuenteespacialtipo fet ON fet.id = fe.tipo;
