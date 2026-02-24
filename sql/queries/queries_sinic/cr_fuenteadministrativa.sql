-- Query para extraer datos de gc_fuenteadministrativa para migrar a cr_fuenteadministrativa
-- Origen: {schema}.gc_fuenteadministrativa
-- Destino: sinic2.cr_fuenteadministrativa
-- Fecha: 2026-02-02

SELECT DISTINCT ON (fa.id)
    fa.id,
    COALESCE(fa.espacio_de_nombres, 'CR_FUENTEADM') as espacio_de_nombres,
    COALESCE(fa.local_id, fa.id::varchar) as local_id,
    fat.text_code as tipo_fuente,
    fa.ente_emisor,
    fa.fecha_documento_fuente,
    fa.descripcion,
    fa.numero_fuente,
    fa.valor_transaccion,
    edt.text_code as estado_disponibilidad
FROM {schema}.gc_fuenteadministrativa fa
LEFT JOIN {schema}.gc_fuenteadministrativatipo fat ON fa.tipo = fat.id
LEFT JOIN {schema}.col_estadodisponibilidadtipo edt ON fa.estado_disponibilidad = edt.id
ORDER BY fa.id;
