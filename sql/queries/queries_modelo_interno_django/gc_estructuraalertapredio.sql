SELECT ex.id, seq, ente_emisor, 
numero_fuente, fecha_documento_fuente, 
fecha_creacion, ge.text_code as estado, gc_predio_alerta, 
gf.text_code as tipo_documento_fuente
FROM {schema}.extalertas ex
JOIN {schema}.gc_predio gp
ON ex.gc_predio_alerta = gp.id
LEFT JOIN {schema}.gc_estadotipo ge ON ge.id = ex.estado
LEFT JOIN {schema}.gc_fuenteadministrativatipo gf on gf.id = ex.tipo_documento_fuente;