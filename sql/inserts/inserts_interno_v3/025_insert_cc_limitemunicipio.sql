-- Insert para la tabla CC_LimiteMunicipio

INSERT INTO {schema}.cc_limitemunicipio (
    t_id,
    t_ili_tid,
    codigo_departamento,
    codigo_municipio,
    nombre_municipio,
    geometria
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    codigo_departamento,
    codigo_municipio,
    nombre_municipio,
    geometria
FROM tmp_cc_limitemunicipio;
