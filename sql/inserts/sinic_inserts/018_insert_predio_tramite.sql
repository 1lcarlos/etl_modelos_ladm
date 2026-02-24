INSERT INTO {schema}.cr_predio_tramitecatastral (
        t_id,
        t_ili_tid,
        cr_predio,
        cr_tramite_catastral)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4 (),
    rp.t_id,
    (SELECT rtc.t_id
     FROM {schema}.cr_tramitecatastral rtc
     WHERE rtc.numero_resolucion = t.resolution_number
     LIMIT 1)
FROM tmp_tramites t
JOIN {schema}.sinic_predio rp ON rp.numero_predial_nacional = t.numero_predial;