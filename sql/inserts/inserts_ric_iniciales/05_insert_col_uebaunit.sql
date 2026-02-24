-- INSERT para tabla col_uebaunit (relacion terreno-predio)
-- Fecha: 2025-12-18

INSERT INTO ric.col_uebaunit (
    t_id, ue_ric_terreno, ue_ric_unidadconstruccion, ue_ric_construccion,
    ue_ric_nu_espaciojuridicoredservicios, ue_ric_nu_espaciojuridicounidadedificacion, baunit
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    (SELECT rt.t_id FROM ric.ric_terreno rt WHERE rt.local_id = cu.ue_gc_terreno::varchar LIMIT 1),
    NULL, NULL, NULL, NULL,
    (SELECT rp.t_id FROM ric.ric_predio rp WHERE rp.local_id = cu.unidad::varchar LIMIT 1)
FROM tmp_col_uebaunit_predio_terreno cu
WHERE cu.ue_gc_terreno IS NOT NULL AND cu.unidad IS NOT NULL
  AND EXISTS (SELECT 1 FROM ric.ric_terreno rt WHERE rt.local_id = cu.ue_gc_terreno::varchar)
  AND EXISTS (SELECT 1 FROM ric.ric_predio rp WHERE rp.local_id = cu.unidad::varchar);
