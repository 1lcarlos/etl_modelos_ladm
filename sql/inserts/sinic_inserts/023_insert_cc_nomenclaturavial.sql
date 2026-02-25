-- Insert para la tabla CC_NomenclaturaVial

INSERT INTO {schema}.cc_nomenclaturavial (
    t_id,
    t_ili_tid,
    tipo_via,
    numero_via,
    geometria
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    cn.t_id as tipo_via,
    numero_via,
    geometria
FROM tmp_cc_nomenclaturavial
JOIN cc_nomenclaturavial_tipo_via cn ON cn.text_code = tmp_cc_nomenclaturavial.tipo_via;
