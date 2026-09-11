-- Vincula gc_predio con gc_tramitecatastral en destino V3
-- El tramite se localiza por numero_resolucion (clave de enlace entre tablas temporales y destino)
INSERT INTO {schema}.gc_predio_tramitecatastral (
    t_id,
    cr_predio,
    cr_tramite_catastral
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    p.t_id,
    tc.t_id
FROM tmp_gc_predio_tramitecatastral tmp
JOIN {schema}.gc_predio p
    ON p.numero_predial_nacional = tmp.predio_numero_predial
JOIN {schema}.gc_tramitecatastral tc
    ON tc.numero_resolucion = tmp.tramite_numero_resolucion
WHERE p.t_id IS NOT NULL
  AND tc.t_id IS NOT NULL;
