-- Insert para la tabla CC_Corregimiento

INSERT INTO {schema}.cc_corregimiento (
    t_id,
    t_basket,
t_ili_tid,
    codigo,
    nombre,
    geometria
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    (SELECT t_id FROM {schema}.t_ili2db_basket WHERE topic = 'Modelo_Interno_ACC_V3.Modelo_Interno' LIMIT 1),
    uuid_generate_v4(),
    codigo,
    nombre,
    geometria
FROM tmp_cc_corregimiento;
