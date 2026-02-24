INSERT INTO ric.ric_predio_tramitecatastral (
        t_id,
        ric_predio,
        ric_tramite_catastral)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    rp.t_id,
    (SELECT rtc.t_id
     FROM ric.ric_tramitecatastral rtc
     WHERE rtc.numero_resolucion = t.resolution_number
     LIMIT 1)
FROM ric.tmp_tramite t
JOIN ric.ric_predio rp ON rp.numero_predial = t.numero_predial;