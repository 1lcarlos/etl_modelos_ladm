-- Insert para la tabla CC_NomenclaturaVial

INSERT INTO {schema}.cc_nomenclaturavial (
    t_id,
    t_basket,
t_ili_tid,
    tipo_via,
    numero_via,
    geometria
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    (SELECT t_id FROM {schema}.t_ili2db_basket WHERE topic = 'Modelo_Interno_ACC_V3.Modelo_Interno' LIMIT 1),
    uuid_generate_v4(),
    cn.t_id as tipo_via,
    numero_via,
    geometria
FROM tmp_cc_nomenclaturavial
JOIN cc_nomenclaturavial_tipo_via cn ON cn.ilicode = tmp_cc_nomenclaturavial.tipo_via;
